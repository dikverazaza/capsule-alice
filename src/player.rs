//! Rust-owned playback supervisor.
//!
//! The Hub owns queue/API/voice/PulseAudio orchestration and now also launches
//! the two external playback clients: capsula-audio (MP3 -> PCM) and paplay
//! (PCM -> the already-running PulseAudio server).  This module never starts,
//! stops, configures, or replaces the PulseAudio daemon itself.

use crate::{
    led_arbiter,
    state::AppState,
    voice_dispatch::VoiceRelayEvent,
    yandex_music,
};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::{
    fs::{self, OpenOptions},
    io,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::Path,
    process::{ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::Command,
    sync::{mpsc, watch},
    time::{sleep, timeout, MissedTickBehavior},
};

const PLAYER_STATE: &str = "/tmp/capsula-yandex-player-state.json";
const AUDIO_DECODER_CANDIDATES: [&str; 3] = [
    "/usr/bin/capsula-audio",
    "/data/capsula-yandex/capsula-audio",
    "/data/capsula-hub/capsula-audio",
];
const MAX_VOICE_FRAME: usize = 4096;
const MAX_PULSE_RESPONSE: usize = 8192;
const MAX_STDERR_BYTES: u64 = 8192;
const PLAYBACK_HEARTBEAT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, PartialEq, Eq)]
struct DecoderFingerprint {
    inode: u64,
    size: u64,
    mtime: i64,
    mtime_nsec: i64,
    mode: u32,
}

static DECODER_CHECK_CACHE: std::sync::OnceLock<std::sync::Mutex<Option<DecoderFingerprint>>> =
    std::sync::OnceLock::new();

fn decoder_cache_matches(cached: Option<DecoderFingerprint>, current: DecoderFingerprint) -> bool {
    cached == Some(current)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayerCommand {
    Wave { from_voice: bool },
    Liked { from_voice: bool },
    StockRadio,
    Track { query: String },
    Playlist { query: String },
    Next,
    Previous,
    Pause,
    Resume,
    Stop,
    Like,
    Dislike,
    WhatIsPlaying,
    SleepTimer { seconds: Option<u64> },
}

static SLEEP_TIMER: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);

pub fn set_sleep_timer(deadline: Option<Instant>) {
    if let Ok(mut g) = SLEEP_TIMER.lock() {
        *g = deadline;
    }
}

pub fn get_sleep_timer() -> Option<Instant> {
    SLEEP_TIMER.lock().ok().and_then(|g| *g)
}

fn format_duration_ru(seconds: u64) -> String {
    if seconds >= 3600 && seconds % 3600 == 0 {
        let hours = seconds / 3600;
        match hours {
            1 => "1 час".to_owned(),
            2..=4 => format!("{hours} часа"),
            _ => format!("{hours} часов"),
        }
    } else {
        let mins = (seconds + 59) / 60;
        let last_two = mins % 100;
        let last = mins % 10;
        if (11..=14).contains(&last_two) {
            format!("{mins} минут")
        } else {
            match last {
                1 => format!("{mins} минуту"),
                2..=4 => format!("{mins} минуты"),
                _ => format!("{mins} минут"),
            }
        }
    }
}

impl PlayerCommand {
    pub fn from_ui(value: &str) -> Option<Self> {
        if let Some(arg) = value.strip_prefix("sleep_timer:") {
            if arg == "cancel" {
                return Some(Self::SleepTimer { seconds: None });
            } else if let Ok(s) = arg.parse::<u64>() {
                return Some(Self::SleepTimer { seconds: Some(s) });
            }
        }
        if let Some(query) = value.strip_prefix("track:").or_else(|| value.strip_prefix("play:")) {
            if query.is_empty() {
                return None;
            }
            return Some(Self::Track { query: query.to_owned() });
        }
        if let Some(query) = value.strip_prefix("playlist:") {
            return Some(Self::Playlist { query: query.to_owned() });
        }
        match value {
            "wave" => Some(Self::Wave { from_voice: false }),
            "liked" => Some(Self::Liked { from_voice: false }),
            "playlist" => Some(Self::Playlist { query: String::new() }),
            "next" => Some(Self::Next),
            "previous" | "prev" => Some(Self::Previous),
            "pause" => Some(Self::Pause),
            "resume" => Some(Self::Resume),
            "stop" | "idle" => Some(Self::Stop),
            "like" => Some(Self::Like),
            "dislike" => Some(Self::Dislike),
            "what_is_playing" => Some(Self::WhatIsPlaying),
            _ => None,
        }
    }

    fn from_voice(value: &str) -> Option<Self> {
        if let Some(arg) = value.strip_prefix("sleep_timer:") {
            if arg == "cancel" {
                return Some(Self::SleepTimer { seconds: None });
            } else if let Ok(s) = arg.parse::<u64>() {
                return Some(Self::SleepTimer { seconds: Some(s) });
            }
        }
        if let Some(query) = value.strip_prefix("track:") {
            if query.is_empty() {
                return None;
            }
            return Some(Self::Track { query: query.to_owned() });
        }
        if let Some(query) = value.strip_prefix("playlist:") {
            return Some(Self::Playlist { query: query.to_owned() });
        }
        match value {
            "wave" => Some(Self::Wave { from_voice: true }),
            "liked" => Some(Self::Liked { from_voice: true }),
            "playlist" => Some(Self::Playlist { query: String::new() }),
            "stock_radio" => Some(Self::StockRadio),
            "next" => Some(Self::Next),
            "previous" => Some(Self::Previous),
            "pause" => Some(Self::Pause),
            "resume" => Some(Self::Resume),
            "stop" => Some(Self::Stop),
            "like" => Some(Self::Like),
            "dislike" => Some(Self::Dislike),
            "what_is_playing" => Some(Self::WhatIsPlaying),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlayerMode {
    Idle,
    External,
    Liked,
    Wave,
    Track,
    Playlist,
}

impl PlayerMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::External => "external",
            Self::Liked => "liked",
            Self::Wave => "wave",
            Self::Track => "track",
            Self::Playlist => "playlist",
        }
    }

    fn is_yandex(self) -> bool {
        matches!(self, Self::Liked | Self::Wave | Self::Track | Self::Playlist)
    }

    fn from_queue(value: &str) -> Self {
        match value {
            "wave" => Self::Wave,
            "liked" => Self::Liked,
            "track" => Self::Track,
            "playlist" => Self::Playlist,
            "external" => Self::External,
            _ => Self::Idle,
        }
    }
}

struct PlaybackResult {
    outcome: String,
    elapsed_seconds: f64,
    shutting_down: bool,
}

struct VoiceCapture {
    phrase_started: Option<Instant>,
    baseline_voice_ids: Vec<String>,
}

impl Default for VoiceCapture {
    fn default() -> Self {
        Self { phrase_started: None, baseline_voice_ids: Vec::new() }
    }
}

pub fn spawn(
    state: AppState,
    command_tx: mpsc::Sender<PlayerCommand>,
    command_rx: mpsc::Receiver<PlayerCommand>,
    shutdown: watch::Receiver<bool>,
) {
    let voice_tx = command_tx.clone();
    tokio::spawn(async move {
        tokio::spawn(voice_relay_loop(voice_tx, shutdown.clone()));
        let shutdown_state = shutdown.clone();
        if let Err(error) = run_engine(state.clone(), command_rx, shutdown).await {
            if *shutdown_state.borrow() {
                tracing::info!("[player] Rust PlaybackSupervisor штатно завершает работу");
                state.runtime.write().await.music_player_state = "остановлен".into();
                write_idle_state("stopped", "idle");
            } else {
                tracing::error!("[player] Rust PlaybackSupervisor остановился: {error:#}");
                state.runtime.write().await.music_player_state = "ошибка".into();
                write_idle_state("error", "idle");
            }
        }
    });
}

async fn run_engine(
    state: AppState,
    mut commands: mpsc::Receiver<PlayerCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!("[player] Rust playback engine запускается; Python playback adapter больше не используется");
    write_idle_state("starting", "idle");
    state.runtime.write().await.music_player_state = "подключение аудио".into();

    let decoder = wait_for_audio_runtime(&mut shutdown).await?;
    tracing::info!("[player] capsula-audio готов: {}; PulseAudio client endpoint обнаружен", decoder);
    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":false})).await {
        tracing::warn!("[PulseAudio] Не удалось сбросить флаг Yandex-active при запуске: {}", safe_error(&error.to_string()));
    }
    state.runtime.write().await.music_player_state = "активен".into();
    write_idle_state("idle", "idle");
    tracing::info!("[player] Rust управляет музыкальной очередью и аудиопроцессами; ожидание команд");

    let mut mode = PlayerMode::Idle;
    let mut current_query: Option<String> = None;
    let mut idle_heartbeat = tokio::time::interval(Duration::from_secs(1));
    idle_heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        if *shutdown.borrow() { break; }
        if !mode.is_yandex() {
            let command = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                    continue;
                }
                _ = idle_heartbeat.tick() => {
                    if let Some(deadline) = get_sleep_timer() {
                        if Instant::now() >= deadline {
                            set_sleep_timer(None);
                            let _ = pulse_request(json!({"op":"set_yandex_volume","volume":100})).await;
                            let _ = led_arbiter::set_screen("sleep", None);
                            tracing::info!("[player] Таймер сна сработал в режиме ожидания: экран переведён в сон u . u");
                        }
                    }
                    if mode == PlayerMode::External {
                        write_idle_state("external", "voice-agent");
                    } else {
                        write_idle_state("idle", "idle");
                    }
                    continue;
                }
                command = commands.recv() => match command {
                    Some(command) => {
                        tracing::info!("[player] Получена команда в run_engine: {:?}", command);
                        command
                    },
                    None => break,
                }
            };
            match command {
                PlayerCommand::SleepTimer { seconds } => {
                    if let Some(s) = seconds {
                        let deadline = Instant::now() + Duration::from_secs(s);
                        set_sleep_timer(Some(deadline));
                        let dur = format_duration_ru(s);
                        let msg = format!("Таймер сна установлен на {dur}. Спокойной ночи.");
                        speak_tts(&decoder, &msg).await;
                        tracing::info!("[player] Таймер сна установлен на {dur} ({s} с)");
                    } else {
                        set_sleep_timer(None);
                        let _ = pulse_request(json!({"op":"set_yandex_volume","volume":100})).await;
                        speak_tts(&decoder, "Таймер сна отменён.").await;
                        tracing::info!("[player] Таймер сна отменён");
                    }
                }
                PlayerCommand::Wave { from_voice } => {
                    tracing::info!("[player] Обработка PlayerCommand::Wave (from_voice: {})", from_voice);
                    if !from_voice {
                        stop_stock_snapshot("запуск Яндекс Музыки из веб-панели").await;
                    }
                    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":true})).await {
                        tracing::error!("[PulseAudio] Не удалось активировать защиту потоков; Wave не запускаю: {}", safe_error(&error.to_string()));
                        write_idle_state("error", "wave");
                        continue;
                    }
                    tracing::info!("[player] Режим переключен на Wave");
                    current_query = None;
                    mode = PlayerMode::Wave;
                }
                PlayerCommand::Liked { from_voice } => {
                    if !from_voice {
                        stop_stock_snapshot("запуск Избранного из веб-панели").await;
                    }
                    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":true})).await {
                        tracing::error!("[PulseAudio] Не удалось активировать защиту потоков; Избранное не запускаю: {}", safe_error(&error.to_string()));
                        write_idle_state("error", "liked");
                        continue;
                    }
                    current_query = None;
                    mode = PlayerMode::Liked;
                }
                PlayerCommand::Track { query } => {
                    stop_stock_snapshot("запуск поиска трека").await;
                    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":true})).await {
                        tracing::error!("[PulseAudio] Не удалось активировать защиту потоков; Track не запускаю: {}", safe_error(&error.to_string()));
                        write_idle_state("error", "track");
                        continue;
                    }
                    current_query = Some(query);
                    mode = PlayerMode::Track;
                }
                PlayerCommand::Playlist { query } => {
                    stop_stock_snapshot("запуск плейлиста").await;
                    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":true})).await {
                        tracing::error!("[PulseAudio] Не удалось активировать защиту потоков; Playlist не запускаю: {}", safe_error(&error.to_string()));
                        write_idle_state("error", "playlist");
                        continue;
                    }
                    current_query = Some(query);
                    mode = PlayerMode::Playlist;
                }
                PlayerCommand::StockRadio => {
                    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":false})).await {
                        tracing::warn!("[PulseAudio] Не удалось сбросить Yandex-active перед штатным радио: {}", safe_error(&error.to_string()));
                    }
                    current_query = None;
                    mode = PlayerMode::External;
                    write_idle_state("external", "voice-agent");
                    tracing::info!("[player] Управление передано штатному радио Маруси");
                }
                PlayerCommand::Stop => {
                    if let Err(error) = pulse_request(json!({"op":"set_yandex_active","active":false})).await {
                        tracing::warn!("[PulseAudio] Не удалось сбросить Yandex-active при остановке: {}", safe_error(&error.to_string()));
                    }
                    current_query = None;
                    mode = PlayerMode::Idle;
                    write_idle_state("idle", "idle");
                }
                PlayerCommand::Next | PlayerCommand::Previous | PlayerCommand::Pause | PlayerCommand::Resume => {
                    // No Yandex stream is active in idle/external mode.
                }
                PlayerCommand::Like | PlayerCommand::Dislike | PlayerCommand::WhatIsPlaying => {
                    speak_tts(&decoder, "Сейчас ничего не играет").await;
                }
            }
            continue;
        }

        let requested_mode = mode.as_str();
        tracing::info!("[player] Переход в run_queue_mode: requested_mode={requested_mode}");
        let next_mode = run_queue_mode(
            requested_mode,
            current_query.as_deref(),
            &decoder,
            &mut commands,
            &mut shutdown,
        ).await;
        tracing::info!("[player] Завершен run_queue_mode: next_mode={next_mode}");
        if let Some(query) = next_mode.strip_prefix("track:") {
            mode = PlayerMode::Track;
            current_query = Some(query.to_owned());
        } else if let Some(query) = next_mode.strip_prefix("playlist:") {
            mode = PlayerMode::Playlist;
            current_query = Some(query.to_owned());
        } else {
            current_query = None;
            mode = PlayerMode::from_queue(&next_mode);
        }
        pulse_request(json!({"op":"set_yandex_active","active":mode.is_yandex()})).await.ok();
        if mode == PlayerMode::External {
            write_idle_state("external", "voice-agent");
            tracing::info!("[player] Штатное радио Маруси получило управление");
        } else if mode == PlayerMode::Idle {
            write_idle_state("idle", "idle");
        }
    }

    pulse_request(json!({"op":"set_yandex_active","active":false})).await.ok();
    write_idle_state("stopped", "idle");
    state.runtime.write().await.music_player_state = "остановлен".into();
    tracing::info!("[player] Rust playback engine остановлен; PulseAudio/voice-agent не затрагивались");
    Ok(())
}

async fn run_queue_mode(
    initial_mode: &str,
    track_query: Option<&str>,
    decoder_path: &str,
    commands: &mut mpsc::Receiver<PlayerCommand>,
    shutdown: &mut watch::Receiver<bool>,
) -> String {
    tracing::info!("[music-queue] Вызов yandex_music::player_queue_start(mode={}, query={:?})", initial_mode, track_query);
    let mut response = match yandex_music::player_queue_start(initial_mode, track_query).await {
        Ok(value) => {
            tracing::info!("[music-queue] player_queue_start успешно вернул результат: {:?}", value);
            value
        },
        Err(error) => {
            tracing::error!("[music-queue] Не удалось запустить режим {initial_mode}: {}", safe_error(&error.to_string()));
            write_idle_state("error", initial_mode);
            return "idle".into();
        }
    };

    loop {
        if *shutdown.borrow() { return "idle".into(); }
        let action = response.get("action").and_then(Value::as_str).unwrap_or("");
        if action == "mode" {
            return response.get("mode").and_then(Value::as_str).unwrap_or("idle").to_owned();
        }
        if action != "play" {
            tracing::error!("[music-queue] Rust queue вернула неизвестное действие");
            return "idle".into();
        }

        let ticket = match response.get("ticket").and_then(Value::as_u64) {
            Some(ticket) => ticket,
            None => {
                tracing::error!("[music-queue] В ответе очереди отсутствует ticket");
                return "idle".into();
            }
        };
        let track = response.get("track").cloned().unwrap_or(Value::Null);
        let audio_url = response.get("audio_url").and_then(Value::as_str).unwrap_or("").to_owned();
        let bitrate = response.get("bitrate_kbps").and_then(Value::as_u64).unwrap_or(0);
        let current_mode = response.get("mode").and_then(Value::as_str).unwrap_or(initial_mode);

        let current_pulse_server = match discover_pulse_server() {
            Ok(server) => server,
            Err(error) => {
                tracing::warn!("[player] PulseAudio socket временно недоступен; повторю после ошибки трека: {}", safe_error(&error.to_string()));
                // Keep the ticket unconsumed. The user can retry after PulseAudio
                // returns; do not tell Rotor that a track was skipped when it
                // never reached the audio pipeline.
                return "idle".into();
            }
        };
        let playback = match play_track(
            decoder_path,
            &current_pulse_server,
            &track,
            &audio_url,
            bitrate,
            ticket,
            current_mode,
            commands,
            shutdown,
        ).await {
            Ok(result) => result,
            Err(error) => {
                tracing::error!("[audio] Ошибка воспроизведения: {}", safe_error(&error.to_string()));
                PlaybackResult { outcome: "error".into(), elapsed_seconds: 0.0, shutting_down: false }
            }
        };

        if playback.shutting_down {
            let _ = yandex_music::player_queue_advance(ticket, "stop", playback.elapsed_seconds).await;
            return "idle".into();
        }

        response = match yandex_music::player_queue_advance(
            ticket,
            &playback.outcome,
            playback.elapsed_seconds,
        ).await {
            Ok(value) => value,
            Err(error) => {
                tracing::error!("[music-queue] Не удалось обновить очередь: {}", safe_error(&error.to_string()));
                return "idle".into();
            }
        };
    }
}

async fn play_track(
    decoder_path: &str,
    pulse_server: &str,
    track: &Value,
    audio_url: &str,
    bitrate: u64,
    ticket: u64,
    mode: &str,
    commands: &mut mpsc::Receiver<PlayerCommand>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<PlaybackResult> {
    let track_id = track.get("id").and_then(Value::as_str).unwrap_or("");
    if track_id.is_empty() || audio_url.len() > 8192 || !audio_url.starts_with("https://") {
        bail!("очередь вернула некорректные данные трека или аудиоссылку");
    }

    let title = display_text(track.get("title").and_then(Value::as_str).unwrap_or("Без названия"), 160);
    let artist = display_text(track.get("artist").and_then(Value::as_str).unwrap_or("Неизвестный исполнитель"), 160);
    let bitrate_text = bitrate.to_string();
    let duration_ms = track.get("duration_ms").and_then(Value::as_u64).unwrap_or(0);
    let cover_uri = track.get("cover_uri").and_then(Value::as_str).unwrap_or("").to_owned();
    let track_id = track.get("id").and_then(Value::as_str).unwrap_or("").to_owned();
    write_player_state("playing", mode, &title, &artist, &bitrate_text, duration_ms, 0, &cover_uri, &track_id);
    let _ = led_arbiter::set_music_active(true);
    tracing::info!("[audio] Воспроизведение: {} — {} ({} kbit/s)", artist, title, bitrate);

    let mut decoder_command = Command::new(decoder_path);
    if let Some(gain) = track.get("normalization_gain").and_then(Value::as_f64).map(|g| g as f32) {
        decoder_command.args(["--gain", &format!("{:.2}", gain)]);
        tracing::info!("[audio] Нормализация громкости трека: {:+.2} dB (ReplayGain/R128)", gain);
    }
    decoder_command
        .arg(audio_url)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut decoder = decoder_command.spawn().context("не удалось запустить capsula-audio")?;
    let decoder_pid = decoder.id().ok_or_else(|| anyhow!("capsula-audio PID недоступен"))?;
    let decoder_stdout = decoder.stdout.take().ok_or_else(|| anyhow!("stdout capsula-audio недоступен"))?;
    let decoder_stderr = decoder.stderr.take().ok_or_else(|| anyhow!("stderr capsula-audio недоступен"))?;

    let mut player_command = Command::new("paplay");
    player_command
        .args([
            "--raw", "--format=s16le", "--rate=44100", "--channels=2",
            "--device=master", "--client-name=yandex-player", "--stream-name=yandex-queue",
            "--latency-msec=1000",
        ])
        .env("PULSE_SERVER", pulse_server)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut player = match player_command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = unsafe { libc::kill(decoder_pid as libc::pid_t, libc::SIGTERM) };
            if timeout(Duration::from_secs(3), decoder.wait()).await.is_err() {
                let _ = unsafe { libc::kill(decoder_pid as libc::pid_t, libc::SIGKILL) };
                let _ = timeout(Duration::from_secs(2), decoder.wait()).await;
            }
            return Err(error).context("не удалось запустить paplay");
        }
    };
    let player_pid = player.id().ok_or_else(|| anyhow!("paplay PID недоступен"))?;
    let player_stdin = player.stdin.take().ok_or_else(|| anyhow!("stdin paplay недоступен"))?;
    let player_stderr = player.stderr.take().ok_or_else(|| anyhow!("stderr paplay недоступен"))?;

    let mut copy_task = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::with_capacity(64 * 1024, decoder_stdout);
        let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, player_stdin);
        let copied = tokio::io::copy_buf(&mut reader, &mut writer).await;
        let _ = tokio::io::AsyncWriteExt::flush(&mut writer).await;
        drop(writer);
        copied
    });
    let mut decoder_stderr_task = tokio::spawn(read_stderr(decoder_stderr));
    let mut player_stderr_task = tokio::spawn(read_stderr(player_stderr));

    let (decoder_status_tx, mut decoder_status_rx) = tokio::sync::oneshot::channel();
    let (player_status_tx, mut player_status_rx) = tokio::sync::oneshot::channel();
    let mut decoder_wait = tokio::spawn(async move { let _ = decoder_status_tx.send(decoder.wait().await); });
    let mut player_wait = tokio::spawn(async move { let _ = player_status_tx.send(player.wait().await); });

    if let Err(error) = pulse_request(json!({"op":"ensure_yandex_audible"})).await {
        tracing::warn!("[PulseAudio] Не удалось проверить mute Яндекс-потока: {}", safe_error(&error.to_string()));
    }
    if let Err(error) = yandex_music::player_mark_track_started(ticket).await {
        tracing::warn!("[rotor] Не удалось зарегистрировать запуск трека: {}", safe_error(&error.to_string()));
    }
    tracing::info!("[audio] Pipeline запущен: capsula-audio PID={decoder_pid}; paplay PID={player_pid}");

    let started_at = Instant::now();
    let mut paused_at: Option<Instant> = None;
    let mut paused_total = Duration::ZERO;
    let mut decoder_done = false;
    let mut player_done = false;
    let mut decoder_status: Option<ExitStatus> = None;
    let mut player_status: Option<ExitStatus> = None;
    let mut transition: Option<String> = None;
    let mut shutting_down = false;
    let mut termination_started_at: Option<Instant> = None;
    let mut decoder_finished_at: Option<Instant> = None;
    let mut player_finished_at: Option<Instant> = None;
    let mut ticker = tokio::time::interval(Duration::from_millis(1000));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    while !(decoder_done && player_done) {
        if transition.is_some() {
            if termination_started_at.is_none() {
                terminate_pair(decoder_pid, player_pid, paused_at.is_some());
                termination_started_at = Some(Instant::now());
            } else if termination_started_at
                .map(|started| started.elapsed() >= Duration::from_secs(3))
                .unwrap_or(false)
            {
                if !decoder_done { signal_one(decoder_pid, libc::SIGKILL); }
                if !player_done { signal_one(player_pid, libc::SIGKILL); }
            }
        }

        tokio::select! {
            result = &mut decoder_status_rx, if !decoder_done => {
                decoder_done = true;
                decoder_finished_at = Some(Instant::now());
                decoder_status = result.ok().and_then(Result::ok);
                if !player_done && decoder_status.as_ref().map(|status| !status.success()).unwrap_or(true) {
                    // EOF closes paplay stdin; the player should drain and exit naturally.
                }
            }
            result = &mut player_status_rx, if !player_done => {
                player_done = true;
                player_finished_at = Some(Instant::now());
                player_status = result.ok().and_then(Result::ok);
                if !decoder_done && transition.is_none()
                    && !player_status.as_ref().map(ExitStatus::success).unwrap_or(false)
                {
                    transition = Some("error".into());
                }
            }
            command = commands.recv(), if transition.is_none() => {
                match command {
                    Some(PlayerCommand::Pause) if paused_at.is_none() && !player_done => {
                        match signal_pair(decoder_pid, player_pid, libc::SIGSTOP) {
                            Ok(()) => {
                                paused_at = Some(Instant::now());
                                if let Err(error) = pulse_request(json!({"op":"set_yandex_muted","muted":true})).await {
                                    tracing::warn!("[player] Не удалось немедленно заглушить sink-input Яндекс-плеера: {}", safe_error(&error.to_string()));
                                }
                                let elapsed = started_at.elapsed().saturating_sub(paused_total).as_secs();
                                write_player_state("paused", mode, &title, &artist, &bitrate_text, duration_ms, elapsed, &cover_uri, &track_id);
                                let _ = led_arbiter::set_music_active(false);
                                tracing::info!("[player] Пауза: SIGSTOP + адресный mute sink-input Яндекс-плеера");
                            }
                            Err(error) => {
                                tracing::warn!("[player] Не удалось синхронно поставить аудиопроцессы на паузу: {error}; завершаю текущий трек безопасно");
                                transition = Some("error".into());
                            }
                        }
                    }
                    Some(PlayerCommand::Resume) if paused_at.is_some() => {
                        // Снимаем паузу ДО размута: процесс должен быть жив, чтобы
                        // sink-input Яндекс-плеера гарантированно существовал при
                        // снятии mute. Иначе размут целится в замороженный/мёртвый
                        // стрим и трек остаётся неслышимым — «продолжи» не работает.
                        match signal_pair(decoder_pid, player_pid, libc::SIGCONT) {
                            Ok(()) => {
                                paused_total += paused_at.take().map(|at| at.elapsed()).unwrap_or_default();
                                let elapsed = started_at.elapsed().saturating_sub(paused_total).as_secs();
                                write_player_state("playing", mode, &title, &artist, &bitrate_text, duration_ms, elapsed, &cover_uri, &track_id);
                                let _ = led_arbiter::set_music_active(true);
                                // Размут с повтором: sink-input мог находиться в
                                // состоянии CORKED/переинициализации сразу после SIGCONT.
                                let mut unmuted = false;
                                for _ in 0..3 {
                                    match pulse_request(json!({"op":"set_yandex_muted","muted":false})).await {
                                        Ok(_) => { unmuted = true; break; }
                                        Err(error) => {
                                             tracing::warn!("[player] Повторная попытка снять mute Яндекс-плеера: {}", safe_error(&error.to_string()));
                                             sleep(Duration::from_millis(150)).await;
                                        }
                                    }
                                }
                                if !unmuted {
                                    tracing::warn!("[player] Не удалось снять mute sink-input Яндекс-плеера за 3 попытки; трек может быть тихим");
                                }
                                tracing::info!("[player] Продолжение: SIGCONT отправлен capsula-audio и paplay; unmute_ok={}", unmuted);
                            }
                            Err(error) => {
                                tracing::warn!("[player] Не удалось продолжить оба аудиопроцесса: {error}; завершаю текущий трек безопасно");
                                transition = Some("error".into());
                            }
                        }
                    }
                    Some(PlayerCommand::Next) => transition = Some("next".into()),
                    Some(PlayerCommand::Previous) => transition = Some("previous".into()),
                    Some(PlayerCommand::Wave { .. }) => transition = Some("wave".into()),
                    Some(PlayerCommand::Liked { .. }) => transition = Some("liked".into()),
                    Some(PlayerCommand::Track { query }) => transition = Some(format!("track:{query}")),
                    Some(PlayerCommand::Playlist { query }) => transition = Some(format!("playlist:{query}")),
                    Some(PlayerCommand::StockRadio) => transition = Some("stock_radio".into()),
                    Some(PlayerCommand::Stop) => transition = Some("stop".into()),
                    Some(PlayerCommand::Like) => {
                        let tid = track_id.clone();
                        tokio::spawn(async move {
                            if let Err(e) = yandex_music::like_track(&tid).await {
                                tracing::warn!("[player] Ошибка добавления в любимые: {e}");
                            } else {
                                tracing::info!("[player] Трек {tid} добавлен в любимые");
                            }
                        });
                        let _ = led_arbiter::show_squint(Duration::from_millis(2500));
                    }
                    Some(PlayerCommand::Dislike) => {
                        let tid = track_id.clone();
                        tokio::spawn(async move {
                            if let Err(e) = yandex_music::dislike_track(&tid).await {
                                tracing::warn!("[player] Ошибка добавления в дизлайки: {e}");
                            } else {
                                tracing::info!("[player] Трек {tid} добавлен в дизлайки");
                            }
                        });
                        transition = Some("dislike".into());
                    }
                    Some(PlayerCommand::WhatIsPlaying) => {
                        let t_artist = artist.clone();
                        let t_title = title.clone();
                        let dec = decoder_path.to_owned();
                        let was_paused = paused_at.is_some();
                        let announcement_start = Instant::now();
                        if !was_paused {
                            let _ = signal_pair(decoder_pid, player_pid, libc::SIGSTOP);
                            let _ = pulse_request(json!({"op":"set_yandex_muted","muted":true})).await;
                        }
                        let text = if t_artist.trim().is_empty() {
                            format!("Сейчас играет {t_title}")
                        } else {
                            format!("Сейчас играет {t_artist}, {t_title}")
                        };
                        speak_tts(&dec, &text).await;
                        if !was_paused {
                            paused_total += announcement_start.elapsed();
                            let _ = signal_pair(decoder_pid, player_pid, libc::SIGCONT);
                            let _ = pulse_request(json!({"op":"set_yandex_muted","muted":false})).await;
                        }
                    }
                    Some(PlayerCommand::SleepTimer { seconds }) => {
                        let dec = decoder_path.to_owned();
                        let was_paused = paused_at.is_some();
                        let announcement_start = Instant::now();
                        if !was_paused {
                            let _ = signal_pair(decoder_pid, player_pid, libc::SIGSTOP);
                            let _ = pulse_request(json!({"op":"set_yandex_muted","muted":true})).await;
                        }
                        if let Some(s) = seconds {
                            let deadline = Instant::now() + Duration::from_secs(s);
                            set_sleep_timer(Some(deadline));
                            let dur = format_duration_ru(s);
                            let msg = format!("Таймер сна установлен на {dur}. Спокойной ночи.");
                            speak_tts(&dec, &msg).await;
                            tracing::info!("[player] Таймер сна установлен на {dur} ({s} с)");
                        } else {
                            set_sleep_timer(None);
                            let _ = pulse_request(json!({"op":"set_yandex_volume","volume":100})).await;
                            speak_tts(&dec, "Таймер сна отменён.").await;
                            tracing::info!("[player] Таймер сна отменён");
                        }
                        if !was_paused {
                            paused_total += announcement_start.elapsed();
                            let _ = signal_pair(decoder_pid, player_pid, libc::SIGCONT);
                            let _ = pulse_request(json!({"op":"set_yandex_muted","muted":false})).await;
                        }
                    }
                    Some(PlayerCommand::Pause) | Some(PlayerCommand::Resume) => {},
                    None => { transition = Some("stop".into()); shutting_down = true; }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    transition = Some("stop".into());
                    shutting_down = true;
                }
            }
            _ = ticker.tick() => {
                if let Some(deadline) = get_sleep_timer() {
                    let now = Instant::now();
                    if now >= deadline {
                        set_sleep_timer(None);
                        let _ = pulse_request(json!({"op":"set_yandex_volume","volume":100})).await;
                        let _ = led_arbiter::set_screen("sleep", None);
                        transition = Some("stop".into());
                        tracing::info!("[player] Таймер сна сработал: плавная остановка музыки и переход экрана в сон u . u");
                    } else {
                        let remaining = deadline - now;
                        if remaining <= Duration::from_secs(30) {
                            let secs = remaining.as_secs_f32();
                            let vol = ((secs / 30.0) * 100.0).round().clamp(0.0, 100.0) as u32;
                            let _ = pulse_request(json!({"op":"set_yandex_volume","volume":vol})).await;
                        }
                    }
                }
                let current_pause = paused_at.map(|at| at.elapsed()).unwrap_or_default();
                let elapsed = started_at.elapsed().saturating_sub(paused_total + current_pause).as_secs();
                write_player_state(
                    if paused_at.is_some() { "paused" } else { "playing" },
                    mode,
                    &title,
                    &artist,
                    &bitrate_text,
                    duration_ms,
                    elapsed,
                    &cover_uri,
                    &track_id,
                );
            }
        }

        if player_done && !decoder_done && transition.is_none()
            && player_finished_at
                .map(|finished| finished.elapsed() > Duration::from_secs(3))
                .unwrap_or(false)
        {
            tracing::warn!("[audio] paplay завершился, но декодер не завершился за 3 секунды");
            transition = Some("error".into());
        }
        if decoder_done && !player_done && transition.is_none() {
            // Normally paplay exits after decoder stdout closes. Give it a short
            // drain window; the loop remains responsive to commands.
            if decoder_finished_at
                .map(|finished| finished.elapsed() > Duration::from_secs(8))
                .unwrap_or(false)
            {
                tracing::warn!("[audio] paplay не завершился после EOF декодера за 8 секунд");
                transition = Some("error".into());
            }
        }
    }

    if paused_at.is_some() {
        paused_total += paused_at.take().map(|at| at.elapsed()).unwrap_or_default();
    }
    let elapsed = started_at.elapsed().saturating_sub(paused_total).as_secs_f64();
    let (copy_join, decoder_stderr_join, player_stderr_join) = tokio::join!(
        timeout(Duration::from_secs(2), &mut copy_task),
        timeout(Duration::from_secs(2), &mut decoder_stderr_task),
        timeout(Duration::from_secs(2), &mut player_stderr_task),
    );
    if copy_join.is_err() { copy_task.abort(); }
    if decoder_stderr_join.is_err() { decoder_stderr_task.abort(); }
    if player_stderr_join.is_err() { player_stderr_task.abort(); }
    let copy_result = copy_join.ok().and_then(Result::ok)
        .unwrap_or_else(|| Err(io::Error::new(io::ErrorKind::TimedOut, "PCM pipe copy timeout")));
    let decoder_stderr = decoder_stderr_join.ok().and_then(Result::ok).unwrap_or_default();
    let player_stderr = player_stderr_join.ok().and_then(Result::ok).unwrap_or_default();
    if timeout(Duration::from_millis(250), &mut decoder_wait).await.is_err() { decoder_wait.abort(); }
    if timeout(Duration::from_millis(250), &mut player_wait).await.is_err() { player_wait.abort(); }

    let outcome = if let Some(outcome) = transition {
        outcome
    } else {
        let decoder_ok = decoder_status.as_ref().map(|status| status.success() || status.signal() == Some(libc::SIGPIPE)).unwrap_or(false);
        let player_ok = player_status.as_ref().map(ExitStatus::success).unwrap_or(false);
        if decoder_ok && player_ok && copy_result.is_ok() {
            "finished".to_owned()
        } else {
            if !copy_result.is_ok() {
                tracing::warn!("[audio] Ошибка передачи PCM в paplay: {}", copy_result.as_ref().err().map(ToString::to_string).unwrap_or_default());
            }
            if !decoder_stderr.is_empty() {
                tracing::warn!("[capsula-audio] {}", safe_error(&String::from_utf8_lossy(&decoder_stderr)));
            }
            if !player_stderr.is_empty() {
                tracing::warn!("[paplay] {}", safe_error(&String::from_utf8_lossy(&player_stderr)));
            }
            "error".to_owned()
        }
    };

    Ok(PlaybackResult { outcome, elapsed_seconds: elapsed.max(0.0), shutting_down })
}

async fn read_stderr<R>(reader: R) -> Vec<u8>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = reader;
    let mut retained = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                let remaining = (MAX_STDERR_BYTES as usize).saturating_sub(retained.len());
                if remaining > 0 {
                    retained.extend_from_slice(&chunk[..count.min(remaining)]);
                }
                // Continue draining even after the retention limit so a noisy
                // child can never block because its stderr pipe is full.
            }
        }
    }
    retained
}

fn signal_one(pid: u32, signal: i32) -> bool {
    if pid == 0 { return false; }
    let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
    result == 0
}

fn signal_pair(decoder_pid: u32, player_pid: u32, signal: i32) -> Result<()> {
    let first = signal_one(decoder_pid, signal);
    let second = signal_one(player_pid, signal);
    if !(first && second) {
        if signal == libc::SIGSTOP {
            if first { signal_one(decoder_pid, libc::SIGCONT); }
            if second { signal_one(player_pid, libc::SIGCONT); }
            bail!("не удалось поставить оба аудиопроцесса на паузу; SIGCONT отправлен для восстановления");
        }
        bail!("не удалось отправить сигнал обоим аудиопроцессам");
    }
    Ok(())
}

fn terminate_pair(decoder_pid: u32, player_pid: u32, paused: bool) {
    if paused {
        signal_one(decoder_pid, libc::SIGCONT);
        signal_one(player_pid, libc::SIGCONT);
    }
    signal_one(decoder_pid, libc::SIGTERM);
    signal_one(player_pid, libc::SIGTERM);
}

async fn wait_for_audio_runtime(shutdown: &mut watch::Receiver<bool>) -> Result<String> {
    let custom_path = std::env::var("CAPSULA_AUDIO_DECODER").ok();
    let mut last_report = Instant::now() - Duration::from_secs(30);
    let mut last_error = String::new();
    loop {
        if *shutdown.borrow() { bail!("остановка Hub во время подготовки аудио"); }
        let mut active_path = None;
        let candidates: Vec<&str> = if let Some(ref p) = custom_path {
            vec![p.as_str()]
        } else {
            AUDIO_DECODER_CANDIDATES.to_vec()
        };
        for path in &candidates {
            if check_decoder(path).await.is_ok() {
                active_path = Some((*path).to_string());
                break;
            }
        }
        let decoder_ready = match &active_path {
            Some(_) => Ok(()),
            None => Err(anyhow!("capsula-audio не найден ни в /data/capsula-hub, ни в /data/capsula-yandex")),
        };
        let pulse_ready = pulse_request(json!({"op":"status"})).await;
        match decoder_ready {
            Ok(()) => match pulse_ready {
                Ok(pulse_status) if pulse_status.get("connected").and_then(Value::as_bool) == Some(true) => {
                    match discover_pulse_server() {
                        Ok(_pulse_server) => return Ok(active_path.unwrap()),
                        Err(error) => last_error = safe_error(&error.to_string()),
                    }
                }
                Ok(_) => last_error = "PulseStreamTracker пока не подключён".into(),
                Err(error) => last_error = safe_error(&error.to_string()),
            },
            Err(error) => last_error = safe_error(&error.to_string()),
        }
        if last_report.elapsed() >= Duration::from_secs(30) {
            if last_error.is_empty() {
                tracing::info!("[player] Ожидание штатного PulseAudio и PulseStreamTracker IPC...");
            } else {
                tracing::warn!("[player] Аудиосреда пока не готова: {last_error}");
            }
            last_report = Instant::now();
        }
        tokio::select! {
            _ = sleep(Duration::from_secs(2)) => {},
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { bail!("остановка Hub во время подготовки аудио"); }
            }
        }
    }
}

async fn check_decoder(path: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("capsula-audio не найден: {path}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        bail!("capsula-audio должен быть исполняемым обычным файлом");
    }
    let fingerprint = DecoderFingerprint {
        inode: metadata.ino(),
        size: metadata.len(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        mode: metadata.mode(),
    };
    let cache = DECODER_CHECK_CACHE.get_or_init(|| std::sync::Mutex::new(None));
    if cache.lock().map(|cached| decoder_cache_matches(*cached, fingerprint)).unwrap_or(false) {
        return Ok(());
    }
    let output = timeout(Duration::from_secs(3), Command::new(path).arg("--version").output())
        .await.context("capsula-audio --version превысил таймаут")??;
    if !output.status.success() || !String::from_utf8_lossy(&output.stdout).contains("capsula-audio") {
        bail!("capsula-audio не прошёл проверку версии");
    }
    if let Ok(mut cached) = cache.lock() {
        *cached = Some(fingerprint);
    }
    Ok(())
}

pub fn discover_pulse_server() -> Result<String> {
    let listing = fs::read_to_string("/proc/net/unix").context("не удалось прочитать /proc/net/unix")?;
    for line in listing.lines() {
        let Some(path) = line.split_whitespace().last() else { continue; };
        if !path.starts_with("/tmp/pulse-") || !path.ends_with("/native") { continue; }
        let metadata = fs::symlink_metadata(path).with_context(|| format!("не удалось проверить PulseAudio socket {path}"))?;
        if metadata.file_type().is_socket() {
            return Ok(format!("unix:{path}"));
        }
    }
    bail!("штатный PulseAudio socket не найден")
}

pub fn url_encode(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len() * 3);
    for b in input.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(encoded, "%{:02X}", b);
            }
        }
    }
    encoded
}

pub async fn speak_tts(decoder_path: &str, text: &str) {
    let pulse_server = match discover_pulse_server() {
        Ok(server) => server,
        Err(err) => {
            tracing::warn!("[tts] PulseAudio socket недоступен для озвучки: {err}");
            return;
        }
    };
    let encoded = url_encode(text);
    let url = format!("https://translate.google.com/translate_tts?ie=UTF-8&client=tw-ob&tl=ru&q={encoded}");
    tracing::info!("[tts] Озвучка: \"{text}\"");

    let mut decoder = match Command::new(decoder_path)
        .arg("--fade-in")
        .arg("0")
        .arg(&url)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            tracing::warn!("[tts] Не удалось запустить {decoder_path}: {err}");
            return;
        }
    };

    let decoder_stdout = match decoder.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = decoder.kill().await;
            return;
        }
    };

    let mut player = match Command::new("paplay")
        .args([
            "--raw",
            "--format=s16le",
            "--rate=44100",
            "--channels=2",
            "--device=master",
            "--client-name=yandex-tts",
            "--stream-name=tts",
        ])
        .env("PULSE_SERVER", &pulse_server)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            tracing::warn!("[tts] Не удалось запустить paplay: {err}");
            let _ = decoder.kill().await;
            return;
        }
    };

    let player_stdin = match player.stdin.take() {
        Some(stdin) => stdin,
        None => {
            let _ = decoder.kill().await;
            let _ = player.kill().await;
            return;
        }
    };

    let copy_task = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::with_capacity(32 * 1024, decoder_stdout);
        let mut writer = tokio::io::BufWriter::with_capacity(32 * 1024, player_stdin);
        let _ = tokio::io::copy_buf(&mut reader, &mut writer).await;
        let _ = tokio::io::AsyncWriteExt::flush(&mut writer).await;
    });

    let _ = timeout(Duration::from_secs(8), async {
        let _ = copy_task.await;
        let _ = player.wait().await;
        let _ = decoder.wait().await;
    }).await;
}

pub async fn speak_tts_custom(text: &str) -> Result<()> {
    let mut chosen = None;
    for cand in &AUDIO_DECODER_CANDIDATES {
        if Path::new(cand).exists() {
            chosen = Some(*cand);
            break;
        }
    }
    let decoder = chosen.unwrap_or("/usr/bin/capsula-audio");
    speak_tts(decoder, text).await;
    Ok(())
}

async fn pulse_request(payload: Value) -> Result<Value> {
    let pulse_path = crate::config::pulse_control_path();
    let mut stream = timeout(Duration::from_secs(2), UnixStream::connect(&pulse_path))
        .await.context("таймаут подключения к PulseStreamTracker")??;
    let mut encoded = serde_json::to_vec(&payload)?;
    if encoded.len() + 1 > MAX_PULSE_RESPONSE {
        bail!("PulseAudio IPC request превышает лимит");
    }
    encoded.push(b'\n');
    timeout(Duration::from_secs(2), stream.write_all(&encoded)).await??;
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();
    timeout(Duration::from_secs(4), reader.take(MAX_PULSE_RESPONSE as u64).read_until(b'\n', &mut line))
        .await.context("таймаут ответа PulseStreamTracker")??;
    if line.is_empty() || line.len() >= MAX_PULSE_RESPONSE || line.last() != Some(&b'\n') {
        bail!("некорректный ответ PulseStreamTracker");
    }
    let response: Value = serde_json::from_slice(&line[..line.len() - 1])
        .context("PulseStreamTracker вернул некорректный JSON")?;
    if response.get("ok").and_then(Value::as_bool) == Some(false) {
        bail!("PulseStreamTracker отклонил команду: {}", response.get("error").and_then(Value::as_str).unwrap_or("unknown"));
    }
    Ok(response)
}

async fn stop_stock_snapshot(reason: &str) {
    let result = async {
        let snapshot = pulse_request(json!({"op":"snapshot"})).await?;
        let ids = snapshot.get("ids").cloned().unwrap_or_else(|| json!([]));
        pulse_request(json!({"op":"stop_snapshot","ids":ids,"reason":reason})).await
    }.await;
    if let Err(error) = result {
        tracing::warn!("[PulseAudio] Не удалось остановить исходные потоки voice-agent: {}", safe_error(&error.to_string()));
    }
}

async fn voice_relay_loop(command_tx: mpsc::Sender<PlayerCommand>, mut shutdown: watch::Receiver<bool>) {
    let mut capture = VoiceCapture::default();
    let relay_path = crate::config::voice_relay_path();
    loop {
        if *shutdown.borrow() { break; }
        let mut stream = match timeout(Duration::from_secs(2), UnixStream::connect(&relay_path)).await {
            Ok(Ok(stream)) => stream,
            _ => {
                tokio::select! {
                    _ = sleep(Duration::from_millis(500)) => {},
                    changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
                }
                continue;
            }
        };
        tracing::info!("[voice] Rust playback controller подключён к typed VoiceEvent relay");
        loop {
            let mut prefix = [0u8; 4];
            let read = tokio::select! {
                result = stream.read_exact(&mut prefix) => result,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                    continue;
                }
            };
            if read.is_err() { break; }
            let size = u32::from_be_bytes(prefix) as usize;
            if size == 0 || size > MAX_VOICE_FRAME { break; }
            let mut payload = vec![0u8; size];
            if stream.read_exact(&mut payload).await.is_err() { break; }
            let event = match serde_json::from_slice::<VoiceRelayEvent>(&payload) {
                Ok(event) => event,
                Err(_) => {
                    tracing::warn!("[voice] Получено некорректное typed VoiceEvent сообщение");
                    continue;
                }
            };
            if let Err(error) = handle_voice_event(event, &command_tx, &mut capture).await {
                tracing::warn!("[voice] Ошибка обработки voice event: {}", safe_error(&error.to_string()));
            }
        }
        tracing::warn!("[voice] VoiceEvent relay отключился; переподключение");
    }
}

async fn handle_voice_event(
    event: VoiceRelayEvent,
    command_tx: &mpsc::Sender<PlayerCommand>,
    capture: &mut VoiceCapture,
) -> Result<()> {
    match event {
        VoiceRelayEvent::PhraseStart { followup } => {
            // A new phrase cancels only the temporary muted-response blank.
            // NightLight is independent and remains active.
            let _ = led_arbiter::end_blank();
            let _ = led_arbiter::activity(Duration::from_secs(8));
            capture.phrase_started = Some(Instant::now());
            capture.baseline_voice_ids = pulse_snapshot().await.unwrap_or_default();
            if followup {
                tracing::debug!("[voice] Continuation phrase: PulseAudio baseline refreshed");
            }
        }
        VoiceRelayEvent::NightLight { on } => {
            // NightLight has higher priority than muted TTS blanking.
            let _ = led_arbiter::set_night_light(on);
            tracing::info!("[night] Ночник {}", if on { "включён" } else { "выключен" });
        }
        VoiceRelayEvent::Screen { mode } => {
            let _ = led_arbiter::set_screen(&mode, None);
            tracing::info!("[screen] Режим экрана: {}", mode);
        }
        VoiceRelayEvent::StockRadio => {
            let _ = led_arbiter::end_blank();
            let _ = led_arbiter::activity(Duration::from_secs(15));
            tracing::info!("[mode] Обнаружено включение штатного радио Маруси");
            let _ = command_tx.send(PlayerCommand::StockRadio).await;
        }
        VoiceRelayEvent::Fallback { phrase, skill, intent } => {
            // Start before voice-agent draws the first speaking.json frame.
            let _ = led_arbiter::activity(Duration::from_secs(15));
            let _ = led_arbiter::begin_blank(Duration::from_secs(5));
            ensure_capture(capture).await;
            request_fallback(capture).await;
            tracing::info!("[filter] VK fallback: skill={}, intent={}, phrase={}", safe_display(&skill, 80), safe_display(&intent, 80), safe_display(&phrase, 100));
        }
        VoiceRelayEvent::Command { command, phrase, skill, intent, vk_fallback } => {
            let is_music_command = matches!(
                command.as_str(),
                "wave" | "liked" | "like" | "dislike" | "what_is_playing" | "next" | "previous" | "pause" | "resume"
            ) || command.starts_with("track:")
                || command.starts_with("playlist:")
                || command.starts_with("sleep_timer:");

            if is_music_command || vk_fallback {
                // Яндекс Музыка перехватывает управление: удерживаем кольцо (blank),
                // чтобы скрыть speaking.json анимацию разговора voice-agent.
                let _ = led_arbiter::begin_blank(Duration::from_secs(6));
            } else {
                // Обычный ответ Маруси (не связанный с Яндекс Музыкой): кольцо может анимироваться.
                let _ = led_arbiter::activity(Duration::from_secs(8));
                let _ = led_arbiter::end_blank();
            }
            ensure_capture(capture).await;
            let action = PlayerCommand::from_voice(&command);
            if matches!(command.as_str(), "wave" | "liked" | "like" | "dislike" | "what_is_playing")
                || command.starts_with("track:")
                || command.starts_with("playlist:")
                || command.starts_with("sleep_timer:")
            {
                request_fallback(capture).await;
                let ids = capture.baseline_voice_ids.clone();
                if !ids.is_empty() {
                    let _ = pulse_request(json!({"op":"stop_snapshot","ids":ids,"reason":"переход на Яндекс Музыку"})).await;
                }
            } else if command == "resume" {
                let age_ms = capture.phrase_started.map(|at| at.elapsed().as_millis() as u64).unwrap_or(0);
                let _ = pulse_request(json!({"op":"guard_stock_resume","age_ms":age_ms,"baseline":capture.baseline_voice_ids})).await;
            } else if command == "stop" {
                let ids = capture.baseline_voice_ids.clone();
                if !ids.is_empty() {
                    let _ = pulse_request(json!({"op":"stop_snapshot","ids":ids,"reason":"команда остановки музыки"})).await;
                }
            }
            tracing::info!("[voice] Rust распознал: {} -> {} (skill={}, intent={})", safe_display(&phrase, 100), command, safe_display(&skill, 80), safe_display(&intent, 80));
            if let Some(action) = action {
                let _ = command_tx.send(action).await;
            }
            capture.phrase_started = None;
            capture.baseline_voice_ids.clear();
        }
    }
    Ok(())
}

async fn ensure_capture(capture: &mut VoiceCapture) {
    if capture.phrase_started.is_none() {
        capture.phrase_started = Some(Instant::now());
        capture.baseline_voice_ids = pulse_snapshot().await.unwrap_or_default();
    }
}

async fn pulse_snapshot() -> Result<Vec<String>> {
    let value = pulse_request(json!({"op":"snapshot"})).await?;
    Ok(value.get("ids").and_then(Value::as_array).into_iter().flatten()
        .filter_map(Value::as_str).filter(|id| id.len() <= 12 && id.bytes().all(|b| b.is_ascii_digit()))
        .take(256).map(str::to_owned).collect())
}

async fn request_fallback(capture: &VoiceCapture) {
    let age_ms = capture.phrase_started.map(|at| at.elapsed().as_millis() as u64).unwrap_or(0);
    let _ = pulse_request(json!({"op":"request_fallback","age_ms":age_ms,"baseline":capture.baseline_voice_ids})).await;
}

fn write_player_state(
    status: &str,
    mode: &str,
    title: &str,
    artist: &str,
    bitrate: &str,
    duration_ms: u64,
    elapsed_sec: u64,
    cover_uri: &str,
    track_id: &str,
) {
    let payload = json!({
        "status": status,
        "state": status,
        "mode": mode,
        "title": safe_display(title, 160),
        "artist": safe_display(artist, 160),
        "bitrate_kbps": bitrate.chars().filter(char::is_ascii_digit).take(5).collect::<String>(),
        "duration": duration_ms / 1000,
        "duration_ms": duration_ms,
        "elapsed": elapsed_sec,
        "cover_uri": cover_uri,
        "track_id": track_id,
        "updated_at": SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
    });
    let parent = Path::new(PLAYER_STATE).parent().unwrap_or_else(|| Path::new("/tmp"));
    if let Err(error) = fs::create_dir_all(parent) {
        tracing::warn!("[player] Не удалось создать каталог состояния: {error}");
        return;
    }
    static STATE_WRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = STATE_WRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let temporary = format!("{PLAYER_STATE}.{}.{}.{}.tmp", std::process::id(), nonce, sequence);
    let encoded = match serde_json::to_vec(&payload) {
        Ok(value) => value,
        Err(error) => { tracing::warn!("[player] Не удалось сериализовать статус: {error}"); return; }
    };
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC).open(&temporary)?;
        use std::io::Write;
        file.write_all(&encoded)?;
        file.sync_all().ok();
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.uid() != unsafe { libc::geteuid() } {
            bail!("небезопасный файл состояния");
        }
        fs::rename(&temporary, PLAYER_STATE)?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        tracing::debug!("[player] Не удалось обновить состояние панели: {error}");
    }
}

fn write_idle_state(status: &str, mode: &str) {
    let _ = led_arbiter::set_music_active(false);
    write_player_state(status, mode, "", "", "", 0, 0, "", "");
}

fn safe_display(value: &str, limit: usize) -> String {
    value.chars().filter(|ch| !ch.is_control()).take(limit).collect()
}

fn display_text(value: &str, limit: usize) -> String {
    safe_display(value, limit)
}

fn safe_error(value: &str) -> String {
    let mut output = String::new();
    let mut index = 0usize;
    while index < value.len() && output.chars().count() < 240 {
        let tail = &value[index..];
        if tail.starts_with("https://") || tail.starts_with("http://") {
            output.push_str("[URL REDACTED]");
            while index < value.len() {
                let ch = value[index..].chars().next().unwrap_or('\0');
                if ch.is_whitespace() || matches!(ch, '"' | '\'' | '<' | '>') { break; }
                index += ch.len_utf8();
            }
            continue;
        }
        let ch = tail.chars().next().unwrap_or('\0');
        if !ch.is_control() { output.push(ch); }
        index += ch.len_utf8();
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{PlayerCommand, PlayerMode};

    #[test]
    fn ui_commands_map_only_to_supported_actions() {
        assert_eq!(PlayerCommand::from_ui("wave"), Some(PlayerCommand::Wave { from_voice: false }));
        assert_eq!(PlayerCommand::from_ui("liked"), Some(PlayerCommand::Liked { from_voice: false }));
        assert_eq!(PlayerCommand::from_ui("stop"), Some(PlayerCommand::Stop));
        assert!(PlayerCommand::from_ui("shell").is_none());
    }

    #[test]
    fn yandex_mode_detection_is_explicit() {
        assert!(PlayerMode::Wave.is_yandex());
        assert!(PlayerMode::Liked.is_yandex());
        assert!(!PlayerMode::External.is_yandex());
        assert!(!PlayerMode::Idle.is_yandex());
        assert_eq!(PlayerMode::from_queue("external"), PlayerMode::External);
        assert_eq!(PlayerMode::from_queue("unexpected"), PlayerMode::Idle);
    }
}


#[cfg(test)]
mod perf_cache_tests {
    use super::*;

    fn fingerprint() -> DecoderFingerprint {
        DecoderFingerprint {
            inode: 10,
            size: 4096,
            mtime: 100,
            mtime_nsec: 20,
            mode: 0o100755,
        }
    }

    #[test]
    fn decoder_cache_requires_full_file_fingerprint_match() {
        let original = fingerprint();
        assert!(decoder_cache_matches(Some(original), original));
        let mut replaced = original;
        replaced.inode += 1;
        assert!(!decoder_cache_matches(Some(original), replaced));
        let mut modified = original;
        modified.mtime_nsec += 1;
        assert!(!decoder_cache_matches(Some(original), modified));
        assert!(!decoder_cache_matches(None, original));
    }
}
