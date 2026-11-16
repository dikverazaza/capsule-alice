//! Rust-side PulseAudio sink-input tracker.
//!
//! This module is a client of the existing PulseAudio daemon. It does not
//! start, stop, configure, or replace PulseAudio, and never changes a master
//! sink. All destructive operations are scoped to a numeric voice-agent
//! sink-input ID discovered from pactl metadata.

use crate::{led_arbiter, state::AppState};
use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    process::Output,
    sync::{Arc, RwLock as StdRwLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    process::Command,
    sync::{Mutex, watch},
    time::{sleep, timeout, MissedTickBehavior},
};

const MAX_REQUEST_BYTES: u64 = 8192;
const MAX_REASON_CHARS: usize = 120;
const MAX_SNAPSHOT_IDS: usize = 256;
const MAX_TIMED_REQUESTS: usize = 64;
const MAX_CONTROL_CLIENTS: usize = 16;
const MAX_RING_BLANK_STREAMS: usize = 64;

#[derive(Clone)]
struct PulseContext {
    server: String,
}

impl PulseContext {
    async fn discover() -> Result<Self> {
        let listing = fs::read_to_string("/proc/net/unix")
            .context("не удалось прочитать /proc/net/unix для поиска PulseAudio")?;
        let socket_path = listing.lines().filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            fields.last().copied()
        }).find(|path| path.starts_with("/tmp/pulse-") && path.ends_with("/native"))
            .ok_or_else(|| anyhow!("штатный PulseAudio socket пока не найден"))?;
        let metadata = fs::symlink_metadata(socket_path)
            .with_context(|| format!("не удалось проверить PulseAudio socket {socket_path}"))?;
        if !metadata.file_type().is_socket() {
            return Err(anyhow!("обнаруженный PulseAudio endpoint не является Unix socket"));
        }
        let context = Self { server: format!("unix:{socket_path}") };
        let output = context.run("pactl", &["info"], Duration::from_secs(5)).await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() || !stdout.contains("Server Name: pulseaudio") {
            return Err(anyhow!("pactl не подтвердил соединение со штатным PulseAudio"));
        }
        Ok(context)
    }

    async fn run(&self, program: &str, args: &[&str], limit: Duration) -> Result<Output> {
        let mut command = Command::new(program);
        command.args(args)
            .env("PULSE_SERVER", &self.server)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let output = timeout(limit, command.output()).await
            .map_err(|_| anyhow!("команда {program} превысила лимит времени"))?
            .with_context(|| format!("не удалось запустить {program}"))?;
        Ok(output)
    }

    async fn run_success(&self, program: &str, args: &[&str], limit: Duration) -> bool {
        self.run(program, args, limit).await.map(|out| out.status.success()).unwrap_or(false)
    }
}

#[derive(Clone)]
struct ActiveStream {
    created: Instant,
}

#[derive(Clone)]
struct TimedRequest {
    id: u64,
    started: Instant,
    baseline: HashSet<String>,
    deadline: Instant,
    min_age: Duration,
}

fn push_bounded_request(queue: &mut Vec<TimedRequest>, request: TimedRequest) {
    if queue.len() >= MAX_TIMED_REQUESTS {
        queue.remove(0);
    }
    queue.push(request);
}

#[derive(Default)]
struct TrackerState {
    active: HashMap<String, ActiveStream>,
    pending_fallback: Vec<TimedRequest>,
    resume_guards: Vec<TimedRequest>,
    yandex_active: bool,
    yandex_sink_input: Option<String>,
    next_request_id: u64,
    ring_blank_streams: HashSet<String>,
}

struct PulseTracker {
    context: PulseContext,
    state: Mutex<TrackerState>,
    refresh_gate: Mutex<()>,
}

impl PulseTracker {
    fn new(context: PulseContext) -> Self {
        let mut state = TrackerState::default();
        // Preserve the previous Python tracker default until the player sends
        // its actual initial mode over IPC.
        state.yandex_active = true;
        Self { context, state: Mutex::new(state), refresh_gate: Mutex::new(()) }
    }

    async fn read_all_sink_blocks(&self) -> Result<HashMap<String, String>> {
        let output = self.context.run("pactl", &["list", "sink-inputs"], Duration::from_secs(4)).await?;
        if !output.status.success() {
            return Err(anyhow!("pactl list sink-inputs завершился с ошибкой"));
        }
        let text = String::from_utf8_lossy(&output.stdout);
        Ok(parse_sink_input_blocks(&text))
    }

    async fn refresh(&self) -> Result<HashMap<String, Instant>> {
        let _refresh = self.refresh_gate.lock().await;
        let blocks = self.read_all_sink_blocks().await?;
        let now = Instant::now();
        let mut state = self.state.lock().await;
        let voice_blocks: HashMap<String, String> = blocks.into_iter()
            .filter(|(_, block)| is_voice_agent_block(block))
            .collect();

        for sid in voice_blocks.keys() {
            state.active.entry(sid.clone()).or_insert(ActiveStream { created: now });
        }
        state.active.retain(|sid, _| voice_blocks.contains_key(sid));
        if state.active.is_empty() {
            state.active = HashMap::new();
        }
        Ok(state.active.iter().map(|(sid, item)| (sid.clone(), item.created)).collect())
    }

    async fn snapshot_voice_ids(&self) -> Result<Vec<String>> {
        let current = self.refresh().await?;
        let mut ids: Vec<String> = current.into_keys().collect();
        ids.sort();
        ids.truncate(MAX_SNAPSHOT_IDS);
        Ok(ids)
    }

    async fn set_yandex_active(&self, active: bool) {
        let mut state = self.state.lock().await;
        state.yandex_active = active;
        if !active {
            state.resume_guards = Vec::new();
            state.yandex_sink_input = None;
        }
    }

    async fn stop_snapshot(&self, ids: Vec<String>, reason: String) -> Result<usize> {
        let current = self.refresh().await?;
        let mut stopped = 0usize;
        let mut unique = HashSet::new();
        for sid in ids.into_iter().take(MAX_SNAPSHOT_IDS) {
            if !is_numeric_id(&sid) || !unique.insert(sid.clone()) || !current.contains_key(&sid) {
                continue;
            }
            if self.kill_voice_stream(&sid, &reason).await {
                stopped += 1;
            }
        }
        if stopped > 0 {
            tracing::info!("[PulseAudio] Адресно остановлено старых voice-agent потоков: {stopped}");
        }
        Ok(stopped)
    }

    async fn start_ring_blank_for_stream(&self, sid: &str) {
        let mut state = self.state.lock().await;
        if !state.ring_blank_streams.contains(sid)
            && state.ring_blank_streams.len() >= MAX_RING_BLANK_STREAMS
        {
            state.ring_blank_streams = HashSet::new();
            tracing::warn!("[RingBlank] Сброшен устаревший список потоков гашения (лимит {MAX_RING_BLANK_STREAMS})");
        }
        state.ring_blank_streams.insert(sid.to_owned());
        if let Err(error) = led_arbiter::begin_blank(Duration::from_secs(8)) {
            state.ring_blank_streams.remove(sid);
            tracing::error!("[RingBlank] Не удалось запустить гашение кольца: {error:#}");
        }
    }

    async fn kill_voice_stream(&self, sid: &str, reason: &str) -> bool {
        if !is_numeric_id(sid) { return false; }

        // Mute first to prevent the first part of a VK fallback prompt from
        // leaking while pacmd performs the targeted removal.
        let muted = self.context.run_success(
            "pactl", &["set-sink-input-mute", sid, "1"], Duration::from_millis(1200),
        ).await;

        let removed = self.context.run_success(
            "pacmd", &["kill-sink-input", sid], Duration::from_millis(1200),
        ).await;

        let muted = if muted || removed {
            muted
        } else {
            // Targeted fallback only. Never mute the master sink or all
            // streams belonging to the voice-agent process.
            self.context.run_success(
                "pactl", &["set-sink-input-mute", sid, "1"], Duration::from_millis(1200),
            ).await
        };

        if muted || removed {
            self.state.lock().await.active.remove(sid);
            let label = if muted { "немедленно заглушён" } else { "удалён" };
            tracing::info!("[PulseAudio] Поток voice-agent #{} {}: {}", sid, label, reason);
            true
        } else {
            false
        }
    }

    async fn guard_stock_resume(&self, age_ms: u64, baseline: Vec<String>) {
        let mut state = self.state.lock().await;
        let id = next_request_id(&mut state);
        push_bounded_request(&mut state.resume_guards, TimedRequest {
            id,
            started: started_from_age(age_ms),
            baseline: bounded_id_set(baseline),
            deadline: Instant::now() + Duration::from_secs(7),
            min_age: Duration::from_millis(2800),
        });
    }

    async fn request_fallback(&self, age_ms: u64, baseline: Vec<String>) {
        let request = {
            let mut state = self.state.lock().await;
            let id = next_request_id(&mut state);
            let request = TimedRequest {
                id,
                started: started_from_age(age_ms),
                baseline: bounded_id_set(baseline),
                deadline: Instant::now() + Duration::from_secs(3),
                min_age: Duration::ZERO,
            };
            push_bounded_request(&mut state.pending_fallback, request.clone());
            request
        };
        let _ = request;
        self.process_pending_fallback().await;
    }

    async fn process_pending_fallback(&self) {
        let current = match self.refresh().await {
            Ok(current) => current,
            Err(error) => {
                tracing::debug!("[PulseAudio] Пока не удалось обновить список потоков: {error}");
                return;
            }
        };
        let now = Instant::now();
        let requests = self.state.lock().await.pending_fallback.clone();
        let mut completed = HashSet::new();
        let mut expired = 0usize;
        let mut claimed = HashSet::new();

        for request in requests {
            let candidate = current.iter()
                .filter(|(sid, created)| {
                    !request.baseline.contains(*sid)
                        && **created >= request.started
                        && !claimed.contains(*sid)
                })
                .max_by_key(|(_, created)| **created)
                .map(|(sid, _)| sid.clone());

            if let Some(sid) = candidate {
                if self.kill_voice_stream(&sid, "ответ VK без подписки").await {
                    claimed.insert(sid);
                    completed.insert(request.id);
                }
            } else if now >= request.deadline {
                completed.insert(request.id);
                expired += 1;
            }
        }

        if !completed.is_empty() {
            let mut state = self.state.lock().await;
            state.pending_fallback.retain(|item| !completed.contains(&item.id));
            if state.pending_fallback.is_empty() {
                state.pending_fallback = Vec::new();
            }
        }
        if expired > 0 {
            tracing::info!("[Фильтр] Не найден отдельный поток ответа VK; остальные потоки не тронуты.");
        }
    }

    async fn process_resume_guards(&self) {
        let (enabled, guards) = {
            let state = self.state.lock().await;
            (state.yandex_active, state.resume_guards.clone())
        };
        if !enabled {
            self.state.lock().await.resume_guards = Vec::new();
            return;
        }
        if guards.is_empty() { return; }

        let current = match self.refresh().await {
            Ok(current) => current,
            Err(error) => {
                tracing::debug!("[PulseAudio] Не удалось проверить возобновлённые потоки: {error}");
                return;
            }
        };
        let now = Instant::now();
        let mut completed = HashSet::new();
        let mut expired = 0usize;
        let mut claimed = HashSet::new();

        for guard in guards {
            let candidate = current.iter()
                .filter(|(sid, created)| {
                    !guard.baseline.contains(*sid)
                        && **created >= guard.started
                        && now.duration_since(**created) >= guard.min_age
                        && !claimed.contains(*sid)
                })
                .max_by_key(|(_, created)| **created)
                .map(|(sid, _)| sid.clone());

            if let Some(sid) = candidate {
                if self.kill_voice_stream(&sid, "VK попытался возобновить своё радио при активной Яндекс Музыке").await {
                    claimed.insert(sid);
                    completed.insert(guard.id);
                }
            } else if now >= guard.deadline {
                completed.insert(guard.id);
                expired += 1;
            }
        }

        if !completed.is_empty() {
            let mut state = self.state.lock().await;
            state.resume_guards.retain(|item| !completed.contains(&item.id));
            if state.resume_guards.is_empty() {
                state.resume_guards = Vec::new();
            }
        }
        if expired > 0 {
            tracing::info!("[Режим] Проверка возобновления VK завершена; лишний поток не обнаружен.");
        }
    }

    async fn set_yandex_muted(&self, muted: bool) -> Result<bool> {
        let cached_id = self.state.lock().await.yandex_sink_input.clone();
        if let Some(sid) = cached_id {
            if self.context.run_success(
                "pactl",
                &["set-sink-input-mute", &sid, if muted { "1" } else { "0" }],
                Duration::from_millis(900),
            ).await {
                return Ok(true);
            }
            self.state.lock().await.yandex_sink_input = None;
        }

        let output = self.context.run(
            "pactl", &["list", "sink-inputs"], Duration::from_millis(1200),
        ).await?;
        if !output.status.success() {
            bail!("pactl list sink-inputs завершился с ошибкой");
        }
        let blocks = parse_sink_input_blocks(&String::from_utf8_lossy(&output.stdout));
        let mut changed = false;
        for (sid, block) in blocks {
            if block.contains("application.name = \"yandex-player\"")
                || block.contains("media.name = \"yandex-queue\"")
            {
                self.state.lock().await.yandex_sink_input = Some(sid.clone());
                let is_muted = block.lines().any(|line| line.trim().eq_ignore_ascii_case("Mute: yes"));
                if is_muted != muted {
                    let value = if muted { "1" } else { "0" };
                    changed |= self.context.run_success(
                        "pactl", &["set-sink-input-mute", &sid, value], Duration::from_millis(900),
                    ).await;
                } else {
                    changed = true;
                }
            }
        }
        Ok(changed)
    }

    async fn set_yandex_volume(&self, volume_percent: u32) -> Result<bool> {
        let vol_clamped = volume_percent.min(100);
        let vol_arg = format!("{}%", vol_clamped);
        let cached_id = self.state.lock().await.yandex_sink_input.clone();
        if let Some(sid) = cached_id {
            if self.context.run_success(
                "pactl",
                &["set-sink-input-volume", &sid, &vol_arg],
                Duration::from_millis(900),
            ).await {
                return Ok(true);
            }
            self.state.lock().await.yandex_sink_input = None;
        }

        let output = self.context.run(
            "pactl", &["list", "sink-inputs"], Duration::from_millis(1200),
        ).await?;
        if !output.status.success() {
            bail!("pactl list sink-inputs завершился с ошибкой");
        }
        let blocks = parse_sink_input_blocks(&String::from_utf8_lossy(&output.stdout));
        let mut changed = false;
        for (sid, block) in blocks {
            if block.contains("application.name = \"yandex-player\"")
                || block.contains("media.name = \"yandex-queue\"")
            {
                self.state.lock().await.yandex_sink_input = Some(sid.clone());
                changed |= self.context.run_success(
                    "pactl", &["set-sink-input-volume", &sid, &vol_arg], Duration::from_millis(900),
                ).await;
            }
        }
        Ok(changed)
    }

    async fn ensure_yandex_stream_audible(&self) -> Result<(bool, bool)> {
        let deadline = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < deadline {
            let blocks = self.read_all_sink_blocks().await?;
            for (sid, block) in blocks {
                if block.contains("application.name = \"yandex-player\"")
                    || block.contains("media.name = \"yandex-queue\"")
                {
                    self.state.lock().await.yandex_sink_input = Some(sid.clone());
                    let is_muted = block.lines().any(|line| {
                        let trimmed = line.trim();
                        trimmed.eq_ignore_ascii_case("Mute: yes")
                    });
                    if is_muted {
                        let unmuted = self.context.run_success(
                            "pactl", &["set-sink-input-mute", &sid, "0"], Duration::from_secs(3),
                        ).await;
                        if unmuted {
                            tracing::info!("[PulseAudio] Снят mute только с потока Яндекс-плеера #{}; громкость не менялась", sid);
                        }
                        return Ok((true, unmuted));
                    }
                    tracing::debug!("[PulseAudio] Поток Яндекс-плеера #{} найден и не заглушён", sid);
                    return Ok((true, false));
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("[PulseAudio] Поток Яндекс-плеера не появился за 1.5 с; продолжение без изменения громкости");
        Ok((false, false))
    }

    async fn status(&self) -> Value {
        let state = self.state.lock().await;
        json!({
            "connected": true,
            "yandex_active": state.yandex_active,
            "tracked_voice_streams": state.active.len(),
            "pending_fallback_checks": state.pending_fallback.len(),
            "resume_guards": state.resume_guards.len(),
        })
    }
}

fn next_request_id(state: &mut TrackerState) -> u64 {
    state.next_request_id = state.next_request_id.wrapping_add(1).max(1);
    state.next_request_id
}

fn started_from_age(age_ms: u64) -> Instant {
    let age = Duration::from_millis(age_ms.min(30_000));
    Instant::now().checked_sub(age + Duration::from_millis(350)).unwrap_or_else(Instant::now)
}

fn bounded_id_set(ids: Vec<String>) -> HashSet<String> {
    ids.into_iter().take(MAX_SNAPSHOT_IDS).filter(|id| is_numeric_id(id)).collect()
}

fn is_numeric_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 12 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_voice_agent_block(block: &str) -> bool {
    block.contains("application.process.binary = \"voice-agent\"")
        || block.contains("application.name = \"Mailru Voice Agent\"")
}

fn parse_sink_input_blocks(text: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();
    let mut current_id: Option<String> = None;
    let mut current_block = String::new();

    let flush = |id: Option<String>, block: &mut String, output: &mut HashMap<String, String>| {
        if let Some(id) = id {
            if is_numeric_id(&id) { output.insert(id, std::mem::take(block)); }
        }
    };

    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(raw_id) = trimmed.strip_prefix("Sink Input #") {
            flush(current_id.take(), &mut current_block, &mut result);
            let id = raw_id.trim();
            current_id = if is_numeric_id(id) { Some(id.to_owned()) } else { None };
        } else if current_id.is_some() {
            current_block.push_str(line);
            current_block.push('\n');
        }
    }
    flush(current_id, &mut current_block, &mut result);
    result
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum PulseRequest {
    Status,
    Snapshot,
    StopSnapshot { ids: Vec<String>, reason: Option<String> },
    SetYandexActive { active: bool },
    GuardStockResume { age_ms: u64, baseline: Vec<String> },
    RequestFallback { age_ms: u64, baseline: Vec<String> },
    EnsureYandexAudible,
    SetYandexMuted { muted: bool },
    SetYandexVolume { volume: u32 },
}

async fn handle_request(manager: Arc<PulseTracker>, request: PulseRequest) -> Value {
    match request {
        PulseRequest::Status => manager.status().await,
        PulseRequest::Snapshot => match manager.snapshot_voice_ids().await {
            Ok(ids) => json!({"ok": true, "ids": ids}),
            Err(error) => json!({"ok": false, "error": compact_error(&error.to_string())}),
        },
        PulseRequest::StopSnapshot { ids, reason } => {
            let reason = reason.unwrap_or_else(|| "voice command".to_owned());
            let reason: String = reason.chars().filter(|ch| !ch.is_control()).take(MAX_REASON_CHARS).collect();
            match manager.stop_snapshot(ids, reason).await {
                Ok(stopped) => json!({"ok": true, "stopped": stopped}),
                Err(error) => json!({"ok": false, "error": compact_error(&error.to_string())}),
            }
        }
        PulseRequest::SetYandexActive { active } => {
            manager.set_yandex_active(active).await;
            json!({"ok": true})
        }
        PulseRequest::GuardStockResume { age_ms, baseline } => {
            manager.guard_stock_resume(age_ms, baseline).await;
            json!({"ok": true})
        }
        PulseRequest::RequestFallback { age_ms, baseline } => {
            manager.request_fallback(age_ms, baseline).await;
            json!({"ok": true})
        }
        PulseRequest::EnsureYandexAudible => match manager.ensure_yandex_stream_audible().await {
            Ok((found, unmuted)) => json!({"ok": true, "found": found, "unmuted": unmuted}),
            Err(error) => json!({"ok": false, "error": compact_error(&error.to_string())}),
        },
        PulseRequest::SetYandexMuted { muted } => match manager.set_yandex_muted(muted).await {
            Ok(found) => json!({"ok": true, "found": found, "muted": muted}),
            Err(error) => json!({"ok": false, "error": compact_error(&format!("set_yandex_muted: {error}"))}),
        },
        PulseRequest::SetYandexVolume { volume } => match manager.set_yandex_volume(volume).await {
            Ok(found) => json!({"ok": true, "found": found, "volume": volume}),
            Err(error) => json!({"ok": false, "error": compact_error(&format!("set_yandex_volume: {error}"))}),
        },
    }
}

fn compact_error(value: &str) -> String {
    value.chars().filter(|ch| !ch.is_control()).take(180).collect()
}

fn remove_stale_socket(path: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            if StdUnixStream::connect(path).is_ok() {
                return Err(anyhow!("PulseAudio control socket уже обслуживается другим процессом"));
            }
            fs::remove_file(path).with_context(|| format!("не удалось удалить старый socket {path}"))?;
            Ok(())
        }
        Ok(_) => Err(anyhow!("{} существует, но не является Unix socket", path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("не удалось проверить {path}")),
    }
}

fn bind_control_socket() -> Result<(UnixListener, u64)> {
    if unsafe { libc::geteuid() } != 0 {
        return Err(anyhow!("PulseAudio tracker IPC должен запускаться от root"));
    }
    let socket_dir = crate::config::run_dir();
    let socket_path = crate::config::pulse_control_path();
    fs::create_dir_all(socket_dir).context("не удалось создать каталог PulseAudio IPC")?;
    let dir_meta = fs::symlink_metadata(socket_dir).context("не удалось проверить каталог PulseAudio IPC")?;
    if !dir_meta.file_type().is_dir() || dir_meta.uid() != 0 {
        return Err(anyhow!("каталог PulseAudio IPC должен быть обычным каталогом root"));
    }
    fs::set_permissions(socket_dir, fs::Permissions::from_mode(0o700))?;
    remove_stale_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).context("не удалось открыть PulseAudio tracker IPC")?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    let inode = fs::symlink_metadata(&socket_path)?.ino();
    Ok((listener, inode))
}

fn remove_socket_if_inode(path: &str, inode: u64) {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_socket() && metadata.ino() == inode {
            let _ = fs::remove_file(path);
        }
    }
}

async fn ipc_client(stream: UnixStream, manager: Arc<PulseTracker>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = Vec::new();
    let read_result = timeout(Duration::from_secs(2), async {
        let mut limited = (&mut reader).take(MAX_REQUEST_BYTES + 1);
        limited.read_until(b'\n', &mut line).await
    }).await;

    let response = match read_result {
        Ok(Ok(size)) if size > 0 && size as u64 <= MAX_REQUEST_BYTES && line.last() == Some(&b'\n') => {
            match serde_json::from_slice::<PulseRequest>(&line[..line.len() - 1]) {
                Ok(request) => handle_request(manager, request).await,
                Err(_) => json!({"ok": false, "error": "invalid_request"}),
            }
        }
        Ok(Ok(_)) => json!({"ok": false, "error": "empty_or_oversized_request"}),
        Ok(Err(_)) | Err(_) => json!({"ok": false, "error": "request_read_failed"}),
    };
    let mut payload = match serde_json::to_vec(&response) {
        Ok(payload) => payload,
        Err(_) => b"{\"ok\":false,\"error\":\"serialization_failed\"}".to_vec(),
    };
    payload.push(b'\n');
    let _ = timeout(Duration::from_secs(2), write_half.write_all(&payload)).await;
}

async fn subscribe_loop(manager: Arc<PulseTracker>, context: PulseContext, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() { break; }
        let mut child = match Command::new("pactl")
            .arg("subscribe")
            .env("PULSE_SERVER", &context.server)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                tracing::warn!("[PulseAudio] Не удалось запустить pactl subscribe: {error}");
                tokio::select! {
                    _ = sleep(Duration::from_secs(2)) => {},
                    changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
                }
                continue;
            }
        };

        tracing::info!("[PulseAudio] Подписка на события sink-input активна");
        let stdout = child.stdout.take();
        if let Some(stdout) = stdout {
            let mut lines = BufReader::new(stdout).lines();
            let _ = manager.refresh().await;
            loop {
                let next = tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { None } else { continue; }
                    }
                    line = timeout(Duration::from_secs(1), lines.next_line()) => {
                        match line {
                            Ok(Ok(value)) => value,
                            Ok(Err(_)) => None,
                            Err(_) => {
                                if child.try_wait().ok().flatten().is_some() { None } else { Some(String::new()) }
                            }
                        }
                    }
                };
                let Some(line) = next else { break; };
                if line.is_empty() { continue; }
                if !line.contains("sink-input") { continue; }
                if let Some(id) = event_sink_input_id(&line) {
                    if line.contains("remove") {
                        // The muted stream is removed by pacmd kill-sink-input
                        // immediately after muting; that is NOT the end of the
                        // speaking animation (101 frames x 40ms = 4.04s). Keep
                        // the flood alive for its bounded deadline instead.
                        let was_blanking = {
                            let mut state = manager.state.lock().await;
                            state.active.remove(&id);
                            state.ring_blank_streams.remove(&id)
                        };
                        if was_blanking {
                            tracing::info!("[RingBlank] Поток ответа удалён; гашение держится до таймаута");
                        }
                    } else if line.contains("new") {
                        for _ in 0..5 {
                            let _ = manager.refresh().await;
                            if manager.state.lock().await.active.contains_key(&id) { break; }
                            sleep(Duration::from_millis(50)).await;
                        }
                        manager.process_pending_fallback().await;
                    }
                }
            }
        }
        let _ = child.kill().await;
        if *shutdown.borrow() { break; }
        tracing::warn!("[PulseAudio] Подписка pactl завершилась; повторное подключение через 2 секунды");
        tokio::select! {
            _ = sleep(Duration::from_secs(2)) => {},
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
        }
    }
}

fn event_sink_input_id(line: &str) -> Option<String> {
    let marker = "sink-input #";
    let index = line.find(marker)? + marker.len();
    let digits: String = line[index..].chars().take_while(|ch| ch.is_ascii_digit()).collect();
    if is_numeric_id(&digits) { Some(digits) } else { None }
}

async fn maintenance_loop(manager: Arc<PulseTracker>, mut shutdown: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = ticker.tick() => {
                let (has_pending, has_guards) = {
                    let state = manager.state.lock().await;
                    (!state.pending_fallback.is_empty(), !state.resume_guards.is_empty())
                };
                if has_pending { manager.process_pending_fallback().await; }
                if has_guards { manager.process_resume_guards().await; }
            }
        }
    }
}

static ACTIVE_TRACKER: StdRwLock<Option<Arc<PulseTracker>>> = StdRwLock::new(None);

fn set_active_tracker(tracker: Option<Arc<PulseTracker>>) {
    if let Ok(mut lock) = ACTIVE_TRACKER.write() {
        *lock = tracker;
    }
}

fn get_active_tracker() -> Option<Arc<PulseTracker>> {
    ACTIVE_TRACKER.read().ok().and_then(|guard| guard.clone())
}

/// Немедленно глушит и останавливает текущий голосовой поток voice-agent (например отказ VK),
/// а также устанавливает защиту на перехват потока ответа VK.
pub async fn suppress_vk_response(reason: &str) {
    if let Some(tracker) = get_active_tracker() {
        let reason_str = reason.to_string();
        if let Ok(ids) = tracker.snapshot_voice_ids().await {
            let _ = tracker.stop_snapshot(ids.clone(), reason_str).await;
            tracker.request_fallback(0, ids).await;
        } else {
            tracker.request_fallback(0, Vec::new()).await;
        }
    }
}

/// Воспроизводит нативный короткий звук подтверждения перехвата команды через PulseAudio
pub fn play_ack_sound() {
    tokio::spawn(async {
        if let Ok(listing) = std::fs::read_to_string("/proc/net/unix") {
            if let Some(socket_path) = listing.lines().filter_map(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                fields.last().copied()
            }).find(|path| path.starts_with("/tmp/pulse-") && path.ends_with("/native")) {
                let _ = tokio::process::Command::new("paplay")
                    .env("PULSE_SERVER", format!("unix:{socket_path}"))
                    .arg("/data/capsula-hub/ack.wav")
                    .output()
                    .await;
            }
        }
    });
}

async fn serve_control_socket(state: AppState, manager: Arc<PulseTracker>, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
    set_active_tracker(Some(manager.clone()));
    let (listener, inode) = bind_control_socket()?;
    let socket_path = crate::config::pulse_control_path();
    tracing::info!("[PulseAudio] Rust tracker готов; IPC: {socket_path}; PulseAudio не перезапускался и не перенастраивался");
    state.runtime.write().await.pulse_state = "активен".into();

    let (worker_tx, worker_rx) = watch::channel(false);
    let clients = Arc::new(tokio::sync::Semaphore::new(MAX_CONTROL_CLIENTS));
    let subscribe = tokio::spawn(subscribe_loop(manager.clone(), manager.context.clone(), worker_rx.clone()));
    let maintenance = tokio::spawn(maintenance_loop(manager.clone(), worker_rx));

    let result = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break Ok(()); }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let permit = match clients.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                tracing::warn!("[PulseAudio] IPC отклонил подключение: достигнут лимит {MAX_CONTROL_CLIENTS} клиентов");
                                drop(stream);
                                continue;
                            }
                        };
                        let manager = manager.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            ipc_client(stream, manager).await;
                        });
                    }
                    Err(error) => break Err(error).context("ошибка Unix IPC PulseAudio tracker"),
                }
            }
        }
    };

    let _ = worker_tx.send(true);
    let _ = timeout(Duration::from_secs(2), subscribe).await;
    let _ = timeout(Duration::from_secs(2), maintenance).await;
    remove_socket_if_inode(&socket_path, inode);
    set_active_tracker(None);
    result
}

pub fn spawn(state: AppState, mut shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() { break; }
            match PulseContext::discover().await {
                Ok(context) => {
                    let manager = Arc::new(PulseTracker::new(context));
                    match serve_control_socket(state.clone(), manager, &mut shutdown).await {
                        Ok(()) => break,
                        Err(error) => {
                            state.runtime.write().await.pulse_state = format!("ошибка: {}", compact_error(&error.to_string()));
                            tracing::error!("[PulseAudio] Rust tracker остановлен: {error:#}");
                        }
                    }
                }
                Err(error) => {
                    state.runtime.write().await.pulse_state = "ожидание PulseAudio".into();
                    tracing::warn!("[PulseAudio] Ожидаю штатный сервер: {}", compact_error(&error.to_string()));
                }
            }
            tokio::select! {
                _ = sleep(Duration::from_secs(3)) => {},
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            }
        }
        state.runtime.write().await.pulse_state = "остановлен".into();
        tracing::info!("[PulseAudio] Rust tracker остановлен; сам PulseAudio daemon не затрагивался");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sink_input_blocks_without_merging_ids() {
        let input = concat!(
            "Sink Input #12\n",
            "    application.name = \"Mailru Voice Agent\"\n",
            "    Mute: no\n",
            "Sink Input #13\n",
            "    application.name = \"yandex-player\"\n",
            "    media.name = \"yandex-queue\"\n",
            "Sink Input #14\n",
            "    application.process.binary = \"voice-agent\"\n",
        );
        let blocks = parse_sink_input_blocks(input);
        assert_eq!(blocks.len(), 3);
        assert!(is_voice_agent_block(&blocks["12"]));
        assert!(!is_voice_agent_block(&blocks["13"]));
        assert!(is_voice_agent_block(&blocks["14"]));
    }

    #[test]
    fn rejects_non_numeric_ids() {
        assert!(is_numeric_id("123"));
        assert!(!is_numeric_id("1;rm"));
        assert!(!is_numeric_id(""));
        assert!(!is_numeric_id("1234567890123"));
    }

    #[test]
    fn parses_subscribe_event_ids() {
        assert_eq!(event_sink_input_id("Event 'new' on sink-input #123"), Some("123".into()));
        assert_eq!(event_sink_input_id("Event 'remove' on sink-input #8"), Some("8".into()));
        assert_eq!(event_sink_input_id("Event on sink #8"), None);
    }

    #[test]
    fn timed_request_vectors_are_hard_bounded() {
        let mut requests = Vec::new();
        for id in 0..(MAX_TIMED_REQUESTS + 9) {
            push_bounded_request(&mut requests, TimedRequest {
                id: id as u64,
                started: Instant::now(),
                baseline: HashSet::new(),
                deadline: Instant::now() + Duration::from_secs(1),
                min_age: Duration::ZERO,
            });
        }
        assert_eq!(requests.len(), MAX_TIMED_REQUESTS);
        assert_eq!(requests.first().unwrap().id, 9);
        assert_eq!(requests.last().unwrap().id, (MAX_TIMED_REQUESTS + 8) as u64);
    }

    #[test]
    fn identifies_voice_agent_by_both_supported_fields() {
        assert!(is_voice_agent_block("application.process.binary = \"voice-agent\""));
        assert!(is_voice_agent_block("application.name = \"Mailru Voice Agent\""));
        assert!(!is_voice_agent_block("application.name = \"yandex-player\""));
    }
}
