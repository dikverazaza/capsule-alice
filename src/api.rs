use crate::{config::{self, PcApp}, deskctl, led_arbiter, player::{self, PlayerCommand}, state::AppState, voice_agent_lapi, wol};
use axum::{extract::State, http::StatusCode, response::{Html, IntoResponse, Response}, Json};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{fs, fs::OpenOptions, io::Read, time::Duration};

pub async fn index() -> Html<&'static str> { Html(include_str!("../web/index.html")) }

pub async fn health(State(state): State<AppState>) -> Json<Value> {
    let cfg = state.config.read().await.clone();
    let runtime = state.runtime.read().await.clone();
    let player_ready = runtime.music_player_state == "активен";
    Json(json!({
        "ok": true,
        "service": "capsula-hub",
        "yandex": {
            "supervisor_running": player_ready,
            "control_ready": player_ready,
            "engine": "Rust",
            "player_state": runtime.music_player_state,
        },
        "voice": {
            "enabled": cfg.voice_enabled,
            "monitor": runtime.voice_monitor_state,
            "devlog": runtime.devlog_state,
            "pulse": runtime.pulse_state,
            "last_voice": runtime.last_voice,
            "last_wake": runtime.last_wake,
            "wake_count": runtime.wake_count,
            "last_error": runtime.last_error,
        },
        "pc": { "configured": !cfg.deskctl_api_key.is_empty() && !cfg.deskctl_url.is_empty() },
        "led_controls": { "enabled": true }
    }))
}

pub async fn get_settings(State(state): State<AppState>) -> Json<Value> {
    let cfg = state.config.read().await.clone();
    Json(json!({
        "device_name": cfg.device_name,
        "mac": cfg.mac,
        "broadcast": cfg.broadcast,
        "port": cfg.port,
        "voice_phrases": cfg.voice_phrases,
        "deskctl_url": cfg.deskctl_url,
        "deskctl_api_key_configured": !cfg.deskctl_api_key.is_empty(),
        "yandex_fifo": "Внутренний канал Rust Hub (FIFO не используется)",
        "voice_log_path": cfg.voice_log_path,
        "voice_enabled": cfg.voice_enabled,
        "led_controls_enabled": true,
        "apps": cfg.apps,
        "llm": {
            "enabled": cfg.llm.enabled,
            "provider": cfg.llm.provider,
            "api_url": cfg.llm.api_url,
            "api_key_configured": !cfg.llm.api_key.is_empty(),
            "model": cfg.llm.model,
            "system_prompt": cfg.llm.system_prompt,
            "max_tokens": cfg.llm.max_tokens,
            "temperature": cfg.llm.temperature,
            "voice_trigger": cfg.llm.voice_trigger,
        }
    }))
}

#[derive(Deserialize)]
pub struct SettingsPatch {
    device_name: Option<String>,
    mac: Option<String>,
    broadcast: Option<String>,
    port: Option<u16>,
    voice_phrases: Option<Vec<String>>,
    deskctl_url: Option<String>,
    deskctl_api_key: Option<String>,
    clear_deskctl_api_key: Option<bool>,
    voice_log_path: Option<String>,
    voice_enabled: Option<bool>,
    apps: Option<Vec<PcApp>>,
    llm_enabled: Option<bool>,
    llm_provider: Option<String>,
    llm_api_url: Option<String>,
    llm_api_key: Option<String>,
    clear_llm_api_key: Option<bool>,
    llm_model: Option<String>,
    llm_system_prompt: Option<String>,
    llm_max_tokens: Option<u32>,
    llm_temperature: Option<f32>,
    llm_voice_trigger: Option<bool>,
}

pub async fn save_settings(State(state): State<AppState>, Json(patch): Json<SettingsPatch>) -> Response {
    let mut stored = state.config.write().await;
    let mut cfg = stored.clone();
    if let Some(v) = patch.device_name { cfg.device_name = v; }
    if let Some(v) = patch.mac { cfg.mac = v; }
    if let Some(v) = patch.broadcast { cfg.broadcast = v; }
    if let Some(v) = patch.port { cfg.port = v; }
    if let Some(v) = patch.voice_phrases { cfg.voice_phrases = v; }
    if let Some(v) = patch.deskctl_url { cfg.deskctl_url = v; }
    if patch.clear_deskctl_api_key.unwrap_or(false) { cfg.deskctl_api_key.clear(); }
    else if let Some(v) = patch.deskctl_api_key {
        if !v.trim().is_empty() { cfg.deskctl_api_key = v.trim().to_owned(); }
    }
    if let Some(v) = patch.voice_log_path { cfg.voice_log_path = v; }
    if let Some(v) = patch.voice_enabled { cfg.voice_enabled = v; }
    if let Some(v) = patch.apps { cfg.apps = v; }
    cfg.led_controls_enabled = true;

    if let Some(v) = patch.llm_enabled { cfg.llm.enabled = v; }
    if let Some(v) = patch.llm_provider { cfg.llm.provider = v; }
    if let Some(v) = patch.llm_api_url { cfg.llm.api_url = v; }
    if patch.clear_llm_api_key.unwrap_or(false) { cfg.llm.api_key.clear(); }
    else if let Some(v) = patch.llm_api_key {
        if !v.trim().is_empty() { cfg.llm.api_key = v.trim().to_owned(); }
    }
    if let Some(v) = patch.llm_model { cfg.llm.model = v; }
    if let Some(v) = patch.llm_system_prompt { cfg.llm.system_prompt = v; }
    if let Some(v) = patch.llm_max_tokens { cfg.llm.max_tokens = v; }
    if let Some(v) = patch.llm_temperature { cfg.llm.temperature = v; }
    if let Some(v) = patch.llm_voice_trigger { cfg.llm.voice_trigger = v; }

    if let Err(error) = cfg.validate() { return api_error(StatusCode::BAD_REQUEST, &error.to_string()); }
    if let Err(error) = config::save(&state.config_path, &cfg) {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    *stored = cfg;
    Json(json!({ "ok": true, "message": "Настройки сохранены" })).into_response()
}

pub async fn wake(State(state): State<AppState>) -> Response {
    let cfg = state.config.read().await.clone();
    match wol::send_wake(&cfg) {
        Ok(()) => Json(json!({"ok": true, "device": cfg.device_name, "broadcast": cfg.broadcast, "port": cfg.port})).into_response(),
        Err(error) => api_error(StatusCode::BAD_REQUEST, &error.to_string()),
    }
}

#[derive(Deserialize)]
pub struct MusicCommand { command: String }

pub async fn internal_pause(State(state): State<AppState>) -> Response {
    let _ = state.music_tx.send(PlayerCommand::Pause).await;
    Json(json!({"ok": true})).into_response()
}

pub async fn music_command(State(state): State<AppState>, Json(input): Json<MusicCommand>) -> Response {
    let Some(command) = PlayerCommand::from_ui(input.command.trim()) else {
        return api_error(StatusCode::BAD_REQUEST, "Неизвестная команда музыки");
    };
    match state.music_tx.send(command).await {
        Ok(()) => Json(json!({ "ok": true, "command": input.command, "engine": "Rust" })).into_response(),
        Err(_) => api_error(StatusCode::SERVICE_UNAVAILABLE, "Rust PlaybackSupervisor недоступен"),
    }
}

/// Read-only telemetry written atomically by the Rust PlaybackSupervisor.
/// This endpoint never opens /dev/log and never controls PulseAudio directly.
#[derive(Deserialize)]
struct PlayerTelemetry {
    #[serde(default)]
    status: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    artist: String,
    #[serde(default)]
    bitrate_kbps: String,
    updated_at: Option<u64>,
}

fn player_status_unavailable(stale: bool) -> Value {
    json!({
        "available": false,
        "status": "unknown",
        "mode": "unknown",
        "title": "",
        "artist": "",
        "bitrate_kbps": "",
        "updated_at": null,
        "stale": stale
    })
}

fn bounded_display_text(value: &str, max_chars: usize) -> String {
    value.chars()
        .filter(|character| !character.is_control())
        .take(max_chars)
        .collect()
}

pub async fn music_status() -> Json<Value> {
    const STATE_PATH: &str = "/tmp/capsula-yandex-player-state.json";
    const MAX_STATE_BYTES: u64 = 16 * 1024;

    // Do not follow a symlink in /tmp. Open first, then inspect the opened
    // descriptor so path replacement cannot redirect the read elsewhere.
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }

    let Ok(file) = options.open(STATE_PATH) else {
        return Json(player_status_unavailable(false));
    };
    let Ok(metadata) = file.metadata() else {
        return Json(player_status_unavailable(true));
    };
    if !metadata.is_file() || metadata.len() > MAX_STATE_BYTES {
        return Json(player_status_unavailable(true));
    }

    let mut raw = Vec::with_capacity(metadata.len() as usize);
    if file.take(MAX_STATE_BYTES + 1).read_to_end(&mut raw).is_err()
        || raw.len() as u64 > MAX_STATE_BYTES
    {
        return Json(player_status_unavailable(true));
    }

    let Ok(state) = serde_json::from_slice::<PlayerTelemetry>(&raw) else {
        return Json(player_status_unavailable(true));
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let stale = state.updated_at
        .map(|timestamp| now.saturating_sub(timestamp) > 30)
        .unwrap_or(true);

    let known_status = matches!(
        state.status.as_str(),
        "playing" | "paused" | "external" | "idle" | "stopped" | "starting" | "error"
    );
    let mut status = if known_status { state.status.as_str() } else { "unknown" };
    if stale && !matches!(status, "idle" | "stopped") {
        status = "unknown";
    }
    let mode = match state.mode.as_str() {
        "wave" | "liked" | "track" | "playlist" | "voice-agent" | "idle" => state.mode.as_str(),
        _ => "unknown",
    };
    let bitrate = state.bitrate_kbps.chars()
        .filter(|character| character.is_ascii_digit())
        .take(5)
        .collect::<String>();

    let sleep_timer_seconds = crate::player::get_sleep_timer().map(|deadline| {
        let now = std::time::Instant::now();
        if deadline > now {
            (deadline - now).as_secs()
        } else {
            0
        }
    });

    Json(json!({
        "available": true,
        "status": status,
        "mode": mode,
        "title": bounded_display_text(&state.title, 160),
        "artist": bounded_display_text(&state.artist, 160),
        "bitrate_kbps": bitrate,
        "updated_at": state.updated_at,
        "stale": stale,
        "sleep_timer_seconds": sleep_timer_seconds
    }))
}


/// Runs a non-playback Yandex API/Rotor preflight. It creates an isolated test
/// session and requests the next batch, but never starts playback or sends
/// Rotor feedback events.
pub async fn yandex_api_probe() -> Response {
    match crate::yandex_api::probe_my_wave().await {
        Ok(report) => Json(report).into_response(),
        Err(error) => api_error(StatusCode::BAD_GATEWAY, &format!("Проверка Yandex API: {error}")),
    }
}

pub async fn pc_status(State(state): State<AppState>) -> Response {
    let cfg = state.config.read().await.clone();
    let status = deskctl::request(&state.http, &cfg, Method::GET, "/api/status", None).await;
    let system = deskctl::request(&state.http, &cfg, Method::GET, "/api/system", None).await;
    let usage = deskctl::request(&state.http, &cfg, Method::GET, "/api/usage", None).await;
    match (status, system, usage) {
        (Ok(status), Ok(system), Ok(usage)) => Json(json!({"ok": true, "status": status, "system": system, "usage": usage})).into_response(),
        (status, system, usage) => {
            let message = status.err().or_else(|| system.err()).or_else(|| usage.err()).map(|e| e.to_string()).unwrap_or_else(|| "Неизвестная ошибка Deskctl".into());
            api_error(StatusCode::BAD_GATEWAY, &message)
        }
    }
}

pub async fn pc_processes(State(state): State<AppState>) -> Response {
    let cfg = state.config.read().await.clone();
    match deskctl::request(&state.http, &cfg, Method::GET, "/api/processes", None).await {
        Ok(value) => Json(json!({"ok": true, "processes": value})).into_response(),
        Err(error) => api_error(StatusCode::BAD_GATEWAY, &error.to_string()),
    }
}

#[derive(Deserialize)]
pub struct LaunchRequest { id: String }

pub async fn pc_launch(State(state): State<AppState>, Json(input): Json<LaunchRequest>) -> Response {
    let cfg = state.config.read().await.clone();
    let Some(app) = cfg.apps.iter().find(|app| app.id == input.id) else {
        return api_error(StatusCode::BAD_REQUEST, "Такого приложения нет в списке");
    };
    if app.path.trim().is_empty() { return api_error(StatusCode::BAD_REQUEST, "Для приложения не задан путь. Заполните его в настройках."); }
    match deskctl::request(&state.http, &cfg, Method::POST, "/api/processes/launch", Some(json!({"path": app.path.clone()}))).await {
        Ok(value) => Json(json!({"ok": true, "result": value})).into_response(),
        Err(error) => api_error(StatusCode::BAD_GATEWAY, &error.to_string()),
    }
}

#[derive(Deserialize)]
pub struct PowerRequest { kind: String }

pub async fn pc_power(State(state): State<AppState>, Json(input): Json<PowerRequest>) -> Response {
    if !["shutdown", "restart", "sleep", "hibernate"].contains(&input.kind.as_str()) {
        return api_error(StatusCode::BAD_REQUEST, "Неизвестное действие питания");
    }
    let cfg = state.config.read().await.clone();
    let path = format!("/api/pw/{}", input.kind);
    match deskctl::request(&state.http, &cfg, Method::POST, &path, Some(json!({}))).await {
        Ok(value) => Json(json!({"ok": true, "result": value})).into_response(),
        Err(error) => api_error(StatusCode::BAD_GATEWAY, &error.to_string()),
    }
}

pub async fn logs() -> Json<Value> {
    let hub = tail_file("/data/capsula-hub/app.log", 100);
    let player = tail_matching_file(
        "/data/capsula-hub/app.log",
        100,
        &["[player]", "[audio]", "[music-queue]", "[rotor]", "[capsula-audio]", "[paplay]"],
    );
    let voice = tail_file("/tmp/capsula-syslog.log", 60);
    Json(json!({"player": player, "hub": hub, "voice": voice}))
}

pub async fn get_led() -> Response {
    match led_arbiter::get_status() {
        Ok((ring, screen)) => Json(json!({
            "ok": true,
            "ring": ring,
            "screen": screen,
        })).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

#[derive(Deserialize, Default)]
pub struct ScreenSubRequest {
    pub mode: Option<String>,
    pub brightness: Option<u8>,
    pub auto_brightness: Option<bool>,
    pub emotion: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct RingSubRequest {
    pub night_light: Option<bool>,
    pub color: Option<String>,
    pub mode: Option<String>,
    pub brightness: Option<u8>,
    pub speed: Option<u8>,
}

#[derive(Deserialize, Default)]
pub struct LedControlRequest {
    target: Option<String>,
    night_light: Option<bool>,
    color: Option<String>,
    mode: Option<String>,
    brightness: Option<u8>,
    speed: Option<u8>,
    reset_override: Option<bool>,
    screen_mode: Option<String>,
    screen_brightness: Option<u8>,
    auto_brightness: Option<bool>,
    emotion: Option<String>,
    screen: Option<ScreenSubRequest>,
    ring: Option<RingSubRequest>,
}

pub async fn set_led(Json(req): Json<LedControlRequest>) -> Response {
    if req.reset_override.unwrap_or(false) {
        let _ = led_arbiter::reset_ring_override();
    }

    let auto_br = req.screen.as_ref().and_then(|s| s.auto_brightness).or(req.auto_brightness);
    if let Some(ab) = auto_br {
        let _ = led_arbiter::set_auto_brightness(ab);
    }
    let emotion = req.screen.as_ref().and_then(|s| s.emotion.clone()).or(req.emotion);
    if let Some(emo) = emotion {
        let _ = led_arbiter::trigger_emotion(&emo, None);
    }

    let target = req.target.as_deref().unwrap_or("both");
    if target == "ring" || target == "both" || req.ring.is_some() {
        let ring_nl = req.ring.as_ref().and_then(|r| r.night_light).or(req.night_light);
        let ring_color = req.ring.as_ref().and_then(|r| r.color.clone()).or(req.color);
        let ring_mode = req.ring.as_ref().and_then(|r| r.mode.clone()).or_else(|| {
            if target == "ring" {
                req.mode.clone()
            } else if req.screen.is_some() || req.screen_mode.is_some() {
                None
            } else {
                req.mode.clone()
            }
        });
        let ring_b = req.ring.as_ref().and_then(|r| r.brightness).or_else(|| {
            if target == "ring" { req.brightness } else { None }
        });
        let ring_speed = req.ring.as_ref().and_then(|r| r.speed).or(req.speed);

        if ring_nl.is_some() || ring_color.is_some() || ring_mode.is_some() || ring_b.is_some() || ring_speed.is_some() {
            if let Err(e) = led_arbiter::set_ring(ring_nl, ring_color, ring_mode, ring_b, ring_speed) {
                return api_error(StatusCode::BAD_REQUEST, &e.to_string());
            }
        }
    }

    let has_screen_sub = req.screen.is_some();
    if target == "screen" || target == "both" || has_screen_sub {
        let screen_mode = req.screen.as_ref().and_then(|s| s.mode.as_deref())
            .or(req.screen_mode.as_deref())
            .or_else(|| {
                if target == "screen" { req.mode.as_deref() } else { None }
            });
        let screen_b = req.screen.as_ref().and_then(|s| s.brightness)
            .or(req.screen_brightness)
            .or_else(|| {
                if target == "screen" { req.brightness } else { None }
            });
        if screen_mode.is_some() || screen_b.is_some() {
            let mode = screen_mode.unwrap_or("clock");
            if let Err(e) = led_arbiter::set_screen(mode, screen_b) {
                return api_error(StatusCode::BAD_REQUEST, &e.to_string());
            }
        }
    }
    get_led().await
}

#[derive(Deserialize)]
pub struct EmotionRequest {
    pub emotion: String,
    pub duration_ms: Option<u64>,
}

pub async fn led_emotion(Json(req): Json<EmotionRequest>) -> Response {
    let dur = req.duration_ms.map(Duration::from_millis);
    match led_arbiter::trigger_emotion(&req.emotion, dur) {
        Ok(()) => Json(json!({"ok": true, "emotion": req.emotion})).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

#[derive(Deserialize)]
pub struct SpeechSayRequest {
    pub text: String,
    pub mode: Option<String>,
    pub engine: Option<String>,
    pub emotion: Option<String>,
}

async fn play_tts_phrase(http: &reqwest::Client, text: &str) -> anyhow::Result<()> {
    let encoded = player::url_encode(text);
    let url = format!("https://translate.google.com/translate_tts?ie=UTF-8&client=tw-ob&tl=ru&q={encoded}");

    let resp = http.get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .send().await
        .map_err(|e| anyhow::anyhow!("Ошибка запроса к TTS: {e}"))?;

    if !resp.status().is_success() {
        anyhow::bail!("TTS сервис вернул HTTP {}", resp.status());
    }

    let bytes = resp.bytes().await
        .map_err(|e| anyhow::anyhow!("Ошибка чтения аудиоданных: {e}"))?;

    let path = "/tmp/tts_speech.mp3";
    fs::write(path, &bytes)
        .map_err(|e| anyhow::anyhow!("Не удалось записать {path}: {e}"))?;

    if let Ok(()) = voice_agent_lapi::play_url(http, "file:/tmp/tts_speech.mp3").await {
        return Ok(());
    }

    player::speak_tts_custom(text).await?;
    Ok(())
}

fn format_marusya_repeat(text: &str) -> String {
    let trimmed = text.trim();
    let lower = trimmed.to_lowercase();
    if lower.starts_with("повтори") || lower.starts_with("повторяй") {
        trimmed.to_string()
    } else {
        format!("повтори фразу {}", trimmed)
    }
}

pub async fn speech_say(State(state): State<AppState>, Json(req): Json<SpeechSayRequest>) -> Response {
    if req.text.trim().is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "Текст не может быть пустым");
    }

    let emo = req.emotion.as_deref().unwrap_or("speaking");
    let _ = led_arbiter::trigger_emotion(emo, Some(Duration::from_secs(4)));

    let mode = req.mode.as_deref()
        .or(req.engine.as_deref())
        .unwrap_or("marusya_voice");

    match mode {
        "command" | "marusya_command" => {
            // Прямой диалог с ассистентом Маруся (чат / команды умного дома / вопросы)
            match voice_agent_lapi::send_text_phrase(&state.http, req.text.trim()).await {
                Ok(()) => Json(json!({"ok": true, "text": req.text, "mode": "command"})).into_response(),
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Ошибка голосового агента: {e}")),
            }
        }
        "tts" | "external" | "google" => {
            // Внешний синтез TTS (MP3)
            match play_tts_phrase(&state.http, &req.text).await {
                Ok(()) => Json(json!({"ok": true, "text": req.text, "mode": "tts"})).into_response(),
                Err(e) => {
                    if let Ok(()) = player::speak_tts_custom(&req.text).await {
                        Json(json!({"ok": true, "text": req.text, "mode": "tts_fallback"})).into_response()
                    } else {
                        api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Ошибка озвучки TTS: {e}"))
                    }
                }
            }
        }
        _ => {
            // НАСТОЯЩЕЕ ОЗВУЧИВАНИЕ ГОЛОСОМ МАРУСИ:
            // Форматирует текст через навык повторения voice-agent,
            // Маруся произносит заданную фразу своим родным голосом!
            let repeat_phrase = format_marusya_repeat(&req.text);
            match voice_agent_lapi::send_text_phrase(&state.http, &repeat_phrase).await {
                Ok(()) => Json(json!({"ok": true, "text": req.text, "mode": "marusya_voice"})).into_response(),
                Err(e) => {
                    tracing::warn!("[speech] Ошибка озвучки Марусей: {e}, пробуем fallback...");
                    match play_tts_phrase(&state.http, &req.text).await {
                        Ok(()) => Json(json!({"ok": true, "text": req.text, "mode": "tts_fallback"})).into_response(),
                        Err(tts_err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Ошибка озвучки: {e}; TTS: {tts_err}")),
                    }
                }
            }
        }
    }
}

pub async fn speech_phrases() -> Response {
    let phrases = voice_agent_lapi::get_native_phrases();
    Json(json!({
        "ok": true,
        "phrases": phrases
    })).into_response()
}

#[derive(Deserialize)]
pub struct SpeechPlayRequest {
    pub url: String,
    pub emotion: Option<String>,
}

pub async fn speech_play(State(state): State<AppState>, Json(req): Json<SpeechPlayRequest>) -> Response {
    if req.url.trim().is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "URL не может быть пустым");
    }

    if let Some(ref emo) = req.emotion {
        let _ = led_arbiter::trigger_emotion(emo, Some(Duration::from_secs(4)));
    }

    match voice_agent_lapi::play_url(&state.http, &req.url).await {
        Ok(()) => Json(json!({"ok": true, "url": req.url})).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Ошибка воспроизведения: {e}")),
    }
}

#[derive(Deserialize)]
pub struct LlmAskRequest {
    pub prompt: String,
    pub system_prompt: Option<String>,
    pub speak: Option<bool>,
    pub emotion: Option<String>,
}

pub async fn llm_ask(State(state): State<AppState>, Json(req): Json<LlmAskRequest>) -> Response {
    if req.prompt.trim().is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "Вопрос для нейросети не может быть пустым");
    }

    let cfg = state.config.read().await.clone();
    if !cfg.llm.enabled {
        return api_error(StatusCode::BAD_REQUEST, "Нейросеть (LLM) отключена в настройках");
    }

    // Включаем эмоцию размышления/поиска на LED-экране
    let _ = led_arbiter::trigger_emotion("search", Some(Duration::from_secs(7)));

    let prompt = req.prompt.trim();
    let sys = req.system_prompt.as_deref();

    let answer = match crate::llm::ask_llm(&state.http, &cfg.llm, prompt, sys).await {
        Ok(ans) => ans,
        Err(e) => {
            let _ = led_arbiter::trigger_emotion("fear", Some(Duration::from_secs(3)));
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Ошибка нейросети: {e}"));
        }
    };

    let speak = req.speak.unwrap_or(true);
    if speak {
        let emo = req.emotion.as_deref().unwrap_or("speaking");
        let _ = led_arbiter::trigger_emotion(emo, Some(Duration::from_secs(6)));
        let phrase = format!("повтори фразу {}", answer);
        if let Err(e) = voice_agent_lapi::send_text_phrase(&state.http, &phrase).await {
            tracing::warn!("[llm] Ошибка озвучивания ответа Марусей: {e}");
        }
    }

    Json(json!({
        "ok": true,
        "prompt": req.prompt,
        "response": answer,
        "spoken": speak,
        "model": crate::llm::resolve_model(&cfg.llm.model, &cfg.llm.provider),
    })).into_response()
}

fn api_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"ok": false, "error": message}))).into_response()
}

fn tail_file(path: &str, max_lines: usize) -> Vec<String> {
    let Ok(content) = fs::read_to_string(path) else { return Vec::new(); };
    content.lines().rev().take(max_lines).collect::<Vec<_>>().into_iter().rev().map(str::to_owned).collect()
}

fn tail_matching_file(path: &str, max_lines: usize, needles: &[&str]) -> Vec<String> {
    let Ok(content) = fs::read_to_string(path) else { return Vec::new(); };
    content.lines()
        .filter(|line| needles.iter().any(|needle| line.contains(needle)))
        .rev().take(max_lines).collect::<Vec<_>>()
        .into_iter().rev().map(str::to_owned).collect()
}
