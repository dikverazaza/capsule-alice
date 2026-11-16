//! Rust owner for the Capsule's `/dev/log` datagram socket.
//!
//! Rust owns /dev/log, parses voice-agent datagrams and relays only compact,
//! typed voice events over a private Unix stream socket using a 4-byte
//! big-endian length prefix. The Rust PlaybackSupervisor consumes those events;
//! the system PulseAudio daemon and штатный voice-agent remain external.

use crate::{led_arbiter, voice_dispatch::{VoiceEventParser, VoiceRelayEvent}};
use anyhow::{anyhow, Context, Result};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::{fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt}, net::UnixStream as StdUnixStream},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{UnixDatagram, UnixListener, UnixStream},
    sync::{broadcast, watch, Semaphore},
    time::{sleep, timeout},
};

const DEVLOG_PATH: &str = "/dev/log";
const LOG_PATH: &str = "/tmp/capsula-syslog.log";
const LOG_BACKUP: &str = "/tmp/capsula-syslog.log.1";
const STATE_PATH: &str = "/tmp/capsula-hub-syslogd.state";
const MAX_LOG_BYTES: u64 = 512 * 1024;
const MAX_DATAGRAM: usize = 65_535;
const MAX_RELAY_EVENT: usize = 4096;
const RELAY_CAPACITY: usize = 256;
const MAX_RELAY_CLIENTS: usize = 4;
const INIT_CANDIDATES: [&str; 6] = [
    "/etc/init.d/S01syslogd",
    "/etc/init.d/S02syslogd",
    "/etc/init.d/S01syslog",
    "/etc/init.d/S02syslog",
    "/etc/init.d/syslogd",
    "/etc/init.d/S01-syslogd",
];

pub fn spawn(state: crate::state::AppState, mut shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                break;
            }

            match DevLogService::start() {
                Ok(mut service) => {
                    tracing::info!("[devlog] Rust получил владение /dev/log; локальный relay: {}", crate::config::voice_relay_path());
                    state.runtime.write().await.devlog_state = "активен".into();
                    let result = service.run(&mut shutdown).await;
                    drop(service);
                    if *shutdown.borrow() {
                        break;
                    }
                    state.runtime.write().await.devlog_state = "перезапуск приёмника".into();
                    if let Err(error) = result {
                        tracing::error!("[devlog] Приёмник остановился: {error:#}");
                    }
                }
                Err(error) => {
                    state.runtime.write().await.devlog_state = format!("ошибка: {}", compact_error(&error.to_string()));
                    tracing::error!("[devlog] Не удалось открыть /dev/log: {error:#}");
                }
            }

            tokio::select! {
                _ = sleep(Duration::from_secs(3)) => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
            }
        }
        state.runtime.write().await.devlog_state = "остановлен".into();
        tracing::info!("[devlog] Приёмник остановлен");
    });
}

struct DevLogService {
    datagram: Option<UnixDatagram>,
    relay_listener: Option<UnixListener>,
    devlog_inode: Option<u64>,
    relay_inode: Option<u64>,
    relay_path: String,
    recovery: SyslogRecovery,
}

impl DevLogService {
    fn start() -> Result<Self> {
        if unsafe { libc::geteuid() } != 0 {
            return Err(anyhow!("приёмник /dev/log должен запускаться от root"));
        }

        // Claim the private relay first. If another live Hub instance owns it,
        // fail before touching /dev/log or the system logger.
        let relay_dir = crate::config::run_dir();
        let relay_path = crate::config::voice_relay_path();
        fs::create_dir_all(relay_dir).context("не удалось создать каталог локального relay")?;
        let relay_dir_meta = fs::symlink_metadata(relay_dir)
            .context("не удалось проверить каталог локального relay")?;
        if !relay_dir_meta.file_type().is_dir() || relay_dir_meta.uid() != 0 {
            return Err(anyhow!("каталог локального relay должен быть обычным каталогом root"));
        }
        fs::set_permissions(relay_dir, fs::Permissions::from_mode(0o700))
            .context("не удалось ограничить доступ к каталогу локального relay")?;
        remove_stale_relay(&relay_path)?;
        let relay_listener = UnixListener::bind(&relay_path)
            .with_context(|| format!("не удалось открыть локальный relay {relay_path}"))?;
        fs::set_permissions(&relay_path, fs::Permissions::from_mode(0o600))
            .context("не удалось ограничить доступ к локальному relay")?;
        let relay_inode = Some(fs::symlink_metadata(&relay_path)?.ino());

        let syslog_running_now = syslog_running()?;
        let previous_state = read_previous_state()?;
        let syslog_was_running = syslog_running_now || previous_state.as_deref() == Some("running");
        let init_script = find_init_script();
        if syslog_was_running && init_script.is_none() {
            return Err(anyhow!("syslogd работает, но init-скрипт для безопасного восстановления не найден"));
        }

        let mut recovery = SyslogRecovery {
            was_running: syslog_was_running,
            init_script,
            devlog_inode: None,
            active: true,
        };
        write_state_marker(syslog_was_running)?;

        if syslog_running_now {
            let script = recovery.init_script.as_ref().expect("checked above");
            tracing::info!("[devlog] Временно останавливаю BusyBox syslogd, чтобы принимать полные сообщения");
            let output = Command::new(script).arg("stop").output()
                .with_context(|| format!("не удалось выполнить {} stop", script.display()))?;
            if syslog_running()? {
                return Err(anyhow!("syslogd продолжает работать после команды stop"));
            }
            if !output.status.success() {
                return Err(anyhow!("init-скрипт syslogd stop завершился с кодом {}", output.status));
            }
            std::thread::sleep(Duration::from_millis(200));
        } else if syslog_was_running {
            tracing::warn!("[devlog] Восстанавливаю состояние: предыдущий приёмник аварийно завершился, syslogd был активен до него");
        } else {
            tracing::info!("[devlog] syslogd уже выключен; его состояние останется выключенным после остановки Rust Hub");
        }

        remove_stale_socket(Path::new(DEVLOG_PATH))?;

        let datagram = UnixDatagram::bind(DEVLOG_PATH)
            .with_context(|| format!("не удалось привязать Unix datagram к {DEVLOG_PATH}"))?;
        let devlog_inode = fs::symlink_metadata(DEVLOG_PATH)?.ino();
        recovery.devlog_inode = Some(devlog_inode);
        fs::set_permissions(DEVLOG_PATH, fs::Permissions::from_mode(0o666))
            .context("не удалось установить права 0666 на /dev/log")?;
        set_receive_buffer(&datagram, 1024 * 1024);

        Ok(Self {
            datagram: Some(datagram),
            relay_listener: Some(relay_listener),
            devlog_inode: Some(devlog_inode),
            relay_inode,
            relay_path,
            recovery,
        })
    }

    async fn run(&mut self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        let listener = self.relay_listener.take()
            .ok_or_else(|| anyhow!("локальный relay listener недоступен"))?;
        let socket = self.datagram.as_ref()
            .ok_or_else(|| anyhow!("/dev/log socket недоступен"))?;
        let (sender, _) = broadcast::channel::<Vec<u8>>(RELAY_CAPACITY);
        let relay_sender = sender.clone();
        let mut relay_shutdown = shutdown.clone();
        let relay_task = tokio::spawn(async move {
            relay_loop(listener, relay_sender, &mut relay_shutdown).await;
        });

        let mut buffer = vec![0u8; MAX_DATAGRAM];
        let mut voice_parser = VoiceEventParser::default();
        let outcome = loop {
            if *shutdown.borrow() { break Ok(()); }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break Ok(()); }
                }
                received = socket.recv(&mut buffer) => {
                    match received {
                        Ok(0) => continue,
                        Ok(size) => {
                            let packet = &buffer[..size];
                            // Parse the full datagram in Rust. The player receives only a
                            // compact typed event; raw ASR/log payloads never cross the relay.
                            let is_va = packet.windows(b"voice-agent".len()).any(|w| w == b"voice-agent");
                            let is_stereo = packet.windows(b"stereoctl".len()).any(|w| w == b"stereoctl");

                            if is_va || is_stereo {
                                // 1. Детектирование регулировки громкости (кнопками, приложением или голосом)
                                let is_volume = packet.windows(b"OnVolumeChangedEvent".len()).any(|w| w == b"OnVolumeChangedEvent")
                                    || packet.windows(b"adjustVolume".len()).any(|w| w == b"adjustVolume")
                                    || packet.windows(b"setVolume".len()).any(|w| w == b"setVolume")
                                    || packet.windows(b"OnSetVolumeRequest".len()).any(|w| w == b"OnSetVolumeRequest");

                                if is_volume {
                                    let _ = led_arbiter::activity(Duration::from_millis(2500));
                                }
                            }

                            if is_va {
                                if let Some(event) = voice_parser.process(packet) {
                                    if let VoiceRelayEvent::NightLight { on } = &event {
                                        tracing::info!(
                                            "[devlog] Typed NightLight event: {}",
                                            if *on { "ON" } else { "OFF" }
                                        );
                                    }
                                    if let VoiceRelayEvent::Screen { mode } = &event {
                                        tracing::info!("[devlog] Typed Screen event: {}", mode);
                                    }
                                    match serde_json::to_vec(&event) {
                                        Ok(encoded) => { let _ = sender.send(encoded); }
                                        Err(error) => tracing::warn!("[voice] Не удалось сериализовать событие: {error}"),
                                    }
                                } else {
                                    // voice_parser вернул None: это не команда плеера, не ночник и не экран.
                                    // Если пришёл OnResponseComplete — это штатный ответ Маруси (погода, часы, факт),
                                    // даём экрану отобразить данные Маруси.
                                    let is_response = packet.windows(b"OnResponseComplete".len()).any(|w| w == b"OnResponseComplete");
                                    if is_response {
                                        let _ = led_arbiter::activity(Duration::from_millis(4000));
                                    }
                                }
                            }
                            // Keep the separate redacted system journal for the WOL monitor
                            // and diagnostics. It is not used by the music command parser.
                            write_bounded_log(packet);
                        }
                        Err(error) => break Err(error).context("ошибка чтения /dev/log"),
                    }
                }
            }
        };
        relay_task.abort();
        let _ = relay_task.await;
        outcome
    }
}

impl Drop for DevLogService {
    fn drop(&mut self) {
        // Drop socket handles before unlinking their filesystem names.
        self.datagram.take();
        self.relay_listener.take();
        remove_socket_if_inode(DEVLOG_PATH, self.devlog_inode);
        remove_socket_if_inode(&self.relay_path, self.relay_inode);
        self.recovery.restore();
    }
}

async fn relay_loop(
    listener: UnixListener,
    sender: broadcast::Sender<Vec<u8>>,
    shutdown: &mut watch::Receiver<bool>,
) {
    let clients = std::sync::Arc::new(Semaphore::new(MAX_RELAY_CLIENTS));
    loop {
        if *shutdown.borrow() { break; }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let permit = match clients.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                tracing::warn!("[devlog] Relay отклонил подключение: достигнут лимит {MAX_RELAY_CLIENTS} клиентов");
                                drop(stream);
                                continue;
                            }
                        };
                        tracing::info!("[devlog] Подключён PulseAudio/music adapter для типизированных событий Маруси");
                        let receiver = sender.subscribe();
                        tokio::spawn(async move {
                            let _permit = permit;
                            relay_client(stream, receiver).await;
                        });
                    }
                    Err(error) => tracing::warn!("[devlog] Ошибка подключения relay-клиента: {error}"),
                }
            }
        }
    }
}

async fn relay_client(mut stream: UnixStream, mut receiver: broadcast::Receiver<Vec<u8>>) {
    loop {
        // Each frame is a compact JSON VoiceRelayEvent, never a raw syslog line.
        let packet = match receiver.recv().await {
            Ok(packet) => packet,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("[devlog] Relay-клиент отстал; потеряно сообщений: {skipped}");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        if packet.len() > MAX_RELAY_EVENT { break; }
        let mut frame = Vec::with_capacity(4 + packet.len());
        frame.extend_from_slice(&(packet.len() as u32).to_be_bytes());
        frame.extend_from_slice(&packet);
        match timeout(Duration::from_secs(2), stream.write_all(&frame)).await {
            Ok(Ok(())) => {}
            _ => break,
        }
    }
}

struct SyslogRecovery {
    was_running: bool,
    init_script: Option<PathBuf>,
    devlog_inode: Option<u64>,
    active: bool,
}

impl SyslogRecovery {
    fn restore(&mut self) {
        if !self.active { return; }
        self.active = false;
        remove_socket_if_inode(DEVLOG_PATH, self.devlog_inode);

        if self.was_running {
            if syslog_running().unwrap_or(false) {
                tracing::info!("[devlog] syslogd уже восстановлен другим процессом");
                let _ = fs::remove_file(STATE_PATH);
                return;
            }
            match self.init_script.as_ref() {
                Some(script) => {
                    tracing::info!("[devlog] Восстанавливаю BusyBox syslogd");
                    match Command::new(script).arg("start").output() {
                        Ok(output) => {
                            std::thread::sleep(Duration::from_millis(300));
                            if syslog_running().unwrap_or(false) {
                                let _ = fs::remove_file(STATE_PATH);
                                tracing::info!("[devlog] BusyBox syslogd восстановлен");
                            } else {
                                tracing::error!("[devlog] syslogd не запустился; marker сохранён для восстановления supervisor-ом: {}", String::from_utf8_lossy(&output.stdout).trim());
                            }
                        }
                        Err(error) => tracing::error!("[devlog] Не удалось запустить syslogd: {error}"),
                    }
                }
                None => tracing::error!("[devlog] Нет init-скрипта для восстановления syslogd; marker сохранён"),
            }
        } else {
            // Preserve the user's pre-existing disabled syslogd state.
            let _ = fs::remove_file(STATE_PATH);
            tracing::info!("[devlog] syslogd был выключен до запуска; оставляю его выключенным");
        }
    }
}

impl Drop for SyslogRecovery {
    fn drop(&mut self) { self.restore(); }
}

fn find_init_script() -> Option<PathBuf> {
    INIT_CANDIDATES.iter().map(PathBuf::from).find(|path| {
        fs::metadata(path).map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0).unwrap_or(false)
    })
}

fn syslog_running() -> Result<bool> {
    if let Ok(output) = Command::new("pidof").arg("syslogd").output() {
        if output.status.success() && !output.stdout.is_empty() { return Ok(true); }
    }
    let output = Command::new("ps").output().context("не удалось выполнить ps для проверки syslogd")?;
    let listing = String::from_utf8_lossy(&output.stdout);
    Ok(listing.lines().any(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("syslogd") && !lower.contains("grep") && !lower.contains("capsula-hub")
    }))
}

fn read_previous_state() -> Result<Option<String>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(STATE_PATH)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("не удалось безопасно открыть marker состояния syslogd"),
    };
    let metadata = file.metadata().context("не удалось проверить marker состояния syslogd")?;
    if !metadata.is_file() || metadata.uid() != 0 {
        return Err(anyhow!("marker состояния syslogd должен быть обычным файлом root"));
    }
    let mut value = String::new();
    let mut bounded = file.take(64);
    bounded.read_to_string(&mut value)
        .context("не удалось прочитать marker состояния syslogd")?;
    let state = value.trim();
    if state == "running" || state == "stopped" { Ok(Some(state.to_owned())) }
    else { Err(anyhow!("неизвестный marker состояния syslogd; автоматический захват /dev/log отменён")) }
}

fn write_state_marker(was_running: bool) -> Result<()> {
    let temporary = format!("{STATE_PATH}.{}.new", std::process::id());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)
        .context("не удалось безопасно создать временный marker состояния syslogd")?;
    file.write_all(if was_running { b"running\n" } else { b"stopped\n" })?;
    file.sync_all().ok();
    fs::rename(&temporary, STATE_PATH).context("не удалось атомарно записать marker syslogd")?;
    Ok(())
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            fs::remove_file(path).with_context(|| format!("не удалось удалить старый сокет {}", path.display()))?;
            Ok(())
        }
        Ok(_) => Err(anyhow!("{} существует, но не является Unix-сокетом", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("не удалось проверить {}", path.display())),
    }
}

fn remove_stale_relay(path: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            if StdUnixStream::connect(path).is_ok() {
                return Err(anyhow!("локальный relay уже обслуживается другим процессом"));
            }
            fs::remove_file(path).with_context(|| format!("не удалось удалить старый relay socket {path}"))?;
            Ok(())
        }
        Ok(_) => Err(anyhow!("{path} существует, но не является Unix-сокетом")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("не удалось проверить {path}")),
    }
}

fn remove_socket_if_inode(path: &str, inode: Option<u64>) {
    let Some(expected) = inode else { return; };
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_socket() && metadata.ino() == expected {
            if let Err(error) = fs::remove_file(path) {
                tracing::warn!("[devlog] Не удалось удалить сокет {path}: {error}");
            }
        }
    }
}

fn set_receive_buffer(socket: &UnixDatagram, bytes: i32) {
    let fd = socket.as_raw_fd();
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &bytes as *const i32 as *const libc::c_void,
            std::mem::size_of::<i32>() as libc::socklen_t,
        )
    };
    if result != 0 {
        tracing::warn!("[devlog] Не удалось увеличить SO_RCVBUF (errno={})", std::io::Error::last_os_error());
    }
}

fn write_bounded_log(packet: &[u8]) {
    let mut text = String::from_utf8_lossy(packet).replace('\0', "");
    text = redact_sensitive_text(&text);
    if text.len() > 6000 {
        let mut end = 6000;
        while !text.is_char_boundary(end) { end -= 1; }
        text.truncate(end);
        text.push_str(" [TRUNCATED FOR LOCAL LOG]");
    }

    let current_size = match fs::symlink_metadata(LOG_PATH) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.uid() == 0 => metadata.len(),
        Ok(_) => {
            tracing::warn!("[devlog] Локальный журнал существует, но не является обычным файлом root; запись пропущена");
            return;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            tracing::warn!("[devlog] Не удалось проверить локальный журнал: {error}");
            return;
        }
    };
    if current_size >= MAX_LOG_BYTES {
        // remove_file unlinks a symlink itself; it never follows the target.
        let _ = fs::remove_file(LOG_BACKUP);
        if let Err(error) = fs::rename(LOG_PATH, LOG_BACKUP) {
            tracing::warn!("[devlog] Не удалось ротировать локальный журнал: {error}");
            return;
        }
    }
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(LOG_PATH)
            .context("не удалось безопасно открыть локальный журнал")?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.uid() != 0 {
            return Err(anyhow!("локальный журнал не принадлежит root или не является обычным файлом"));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(text.trim_end().as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    })();
    if let Err(error) = result {
        // Logging must never block or stop voice-event delivery.
        tracing::warn!("[devlog] Не удалось записать диагностическое сообщение: {error}");
    }
}

fn redact_sensitive_text(input: &str) -> String {
    let mut text = input.to_owned();

    // Keep parity with the previous Python redactor: signed audio URLs are
    // removed as a whole because credentials may be encoded in path segments.
    text = redact_spans(&text, &["https://", "http://"], |ch| {
        !ch.is_whitespace() && !matches!(ch, '"' | '\'' | '<' | '>')
    }, "[URL REDACTED]");

    // Query-like key=value fragments (the old filter used the same delimiter set).
    for key in [
        "access_token", "refresh_token", "oauth_token", "token", "sign",
        "session_id", "device_id", "home_id", "room_id", "coordinates",
    ] {
        let needle = format!("{key}=");
        let mut cursor = 0usize;
        loop {
            let lower = text.to_ascii_lowercase();
            let Some(relative) = lower.get(cursor..).and_then(|tail| tail.find(&needle)) else { break; };
            let start = cursor + relative;
            let value_start = start + needle.len();
            let value_end = text[value_start..]
                .find(|ch: char| ch.is_whitespace() || matches!(ch, '&' | ',' | '}' | ']' | '"' | '\''))
                .map(|offset| value_start + offset)
                .unwrap_or(text.len());
            if value_end <= value_start { break; }
            text.replace_range(value_start..value_end, "[REDACTED]");
            cursor = value_start + "[REDACTED]".len();
            if cursor >= text.len() { break; }
        }
    }

    // JSON values are quoted; preserve the key and JSON shape while hiding the value.
    for key in ["access_token", "oauth_token", "refresh_token", "token", "sign"] {
        let needle = format!("\"{key}\"");
        let mut cursor = 0usize;
        loop {
            let lower = text.to_ascii_lowercase();
            let Some(relative) = lower.get(cursor..).and_then(|tail| tail.find(&needle)) else { break; };
            let key_start = cursor + relative;
            let after_key = key_start + needle.len();
            let Some(colon_rel) = text[after_key..].find(':') else { break; };
            let after_colon = after_key + colon_rel + 1;
            let Some(quote_rel) = text[after_colon..].find('"') else { break; };
            let value_start = after_colon + quote_rel;
            let bytes = text.as_bytes();
            let mut index = value_start + 1;
            let mut escaped = false;
            let mut value_end = None;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' if !escaped => escaped = true,
                    b'"' if !escaped => { value_end = Some(index); break; }
                    _ => escaped = false,
                }
                index += 1;
            }
            let Some(value_end) = value_end else { break; };
            text.replace_range(value_start + 1..value_end, "[REDACTED]");
            cursor = value_start + 1 + "[REDACTED]".len();
            if cursor >= text.len() { break; }
        }
    }

    // Authorization headers are occasionally emitted by low-level HTTP clients.
    text = redact_bearer_tokens(&text);
    text
}

fn redact_spans<F>(input: &str, prefixes: &[&str], keep: F, replacement: &str) -> String
where
    F: Fn(char) -> bool,
{
    let mut text = input.to_owned();
    let mut cursor = 0usize;
    loop {
        let lower = text.to_ascii_lowercase();
        let next = prefixes.iter().filter_map(|prefix| {
            lower.get(cursor..)?.find(*prefix).map(|offset| (cursor + offset, *prefix))
        }).min_by_key(|(index, _)| *index);
        let Some((start, prefix)) = next else { break; };
        let value_start = start + prefix.len();
        let value_end = text[value_start..].char_indices()
            .find(|(_, ch)| !keep(*ch))
            .map(|(offset, _)| value_start + offset)
            .unwrap_or(text.len());
        if value_end <= value_start {
            cursor = value_start;
            if cursor >= text.len() { break; }
            continue;
        }
        text.replace_range(start..value_end, replacement);
        cursor = start + replacement.len();
        if cursor >= text.len() { break; }
    }
    text
}

fn redact_bearer_tokens(input: &str) -> String {
    let mut text = input.to_owned();
    let mut cursor = 0usize;
    loop {
        let lower = text.to_ascii_lowercase();
        let Some(relative) = lower.get(cursor..).and_then(|tail| tail.find("authorization")) else { break; };
        let start = cursor + relative;
        let after_key = start + "authorization".len();
        let Some(colon_rel) = text[after_key..].find(':') else { break; };
        let after_colon = after_key + colon_rel + 1;
        let remaining = &text[after_colon..];
        let leading = remaining.len() - remaining.trim_start().len();
        let bearer_start = after_colon + leading;
        let lower_after = text[bearer_start..].to_ascii_lowercase();
        if !lower_after.starts_with("bearer") {
            cursor = after_key;
            if cursor >= text.len() { break; }
            continue;
        }
        let token_start = bearer_start + "bearer".len();
        let Some(token_offset) = text[token_start..].find(|ch: char| !ch.is_whitespace()) else { break; };
        let value_start = token_start + token_offset;
        let value_end = text[value_start..].find(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | '<' | '>'))
            .map(|offset| value_start + offset)
            .unwrap_or(text.len());
        if value_end <= value_start { break; }
        text.replace_range(value_start..value_end, "[REDACTED]");
        cursor = value_start + "[REDACTED]".len();
        if cursor >= text.len() { break; }
    }
    text
}

fn compact_error(value: &str) -> String {
    value.chars().filter(|ch| !ch.is_control()).take(100).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_query_parameters_and_json_tokens() {
        let source = "url?device_id=abc&sign=secret token=xyz {\"access_token\": \"oauth-secret\", \"refresh_token\":\"refresh-secret\"}";
        let safe = redact_sensitive_text(source);
        assert!(safe.contains("device_id=[REDACTED]"));
        assert!(safe.contains("sign=[REDACTED]"));
        assert!(safe.contains("token=[REDACTED]"));
        assert!(safe.contains("\"access_token\": \"[REDACTED]\""));
        assert!(!safe.contains("oauth-secret"));
        assert!(!safe.contains("refresh-secret"));
    }

    #[test]
    fn redacts_signed_urls_and_bearer_headers() {
        let source = "GET https://storage.example/audio/path?sign=secret Authorization: Bearer abc.def.ghi";
        let safe = redact_sensitive_text(source);
        assert!(safe.contains("[URL REDACTED]"));
        assert!(safe.contains("Authorization: Bearer [REDACTED]"));
        assert!(!safe.contains("storage.example"));
        assert!(!safe.contains("secret"));
        assert!(!safe.contains("abc.def.ghi"));
    }

    #[test]
    fn preserves_public_log_text() {
        assert_eq!(redact_sensitive_text("voice-agent OnResponseComplete OK"), "voice-agent OnResponseComplete OK");
    }

    #[test]
    fn truncates_no_unicode_scalar() {
        let mut value = "я".repeat(4000);
        if value.len() > 6000 {
            let mut end = 6000;
            while !value.is_char_boundary(end) { end -= 1; }
            value.truncate(end);
        }
        assert!(value.is_char_boundary(value.len()));
    }
}
