use capsula_lib::*;

use anyhow::{Context, Result};
use axum::{middleware, routing::{get, post}, Router};
use reqwest::Client;
use state::AppState;
use std::{env, fs, path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::{mpsc, watch, RwLock}};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        println!("capsula-hub {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--check-credentials") {
        yandex_music::validate_credentials_file()?;
        println!("credentials=ok");
        return Ok(());
    }
    let config_path = PathBuf::from(config::CONFIG_PATH);
    let auth_path = PathBuf::from(config::AUTH_PATH);
    if let Some(parent) = config_path.parent() { fs::create_dir_all(parent)?; }

    // Start the only Hub-owned LED writer before restoring persisted state.
    // This does not restart or reconfigure voice-agent or PulseAudio.
    led_arbiter::start()?;
    ring_blank::restore_night_light();

    let force_auth_reset = args.iter().any(|arg| arg == "--reset-auth");
    let (auth_data, generated_password) = auth::ensure_auth(&auth_path, force_auth_reset)?;
    if let Some(ref password) = generated_password {
        let first_login = PathBuf::from("/data/capsula-hub/first-login.txt");
        fs::write(&first_login, format!("Пользователь: admin\nПароль: {password}\nУдалите этот файл после первого входа.\n"))?;
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&first_login, fs::Permissions::from_mode(0o600))?;
        }
        tracing::warn!("Создан новый пароль панели. Он сохранён в /data/capsula-hub/first-login.txt (права 600).");
    }

    if force_auth_reset || args.iter().any(|arg| arg == "--init-auth") {
        if let Some(password) = generated_password { println!("admin:{password}"); }
        else { println!("Авторизация уже настроена; существующий пароль не менялся."); }
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--check") {
        let cfg = config::AppConfig::load_or_create(&config_path)?;
        println!("config=ok; auth=ok; music_engine=rust; voice_log={}", cfg.voice_log_path);
        return Ok(());
    }

    let cfg = config::AppConfig::load_or_create(&config_path)?;
    let (music_tx, music_rx) = mpsc::channel(128);
    let state = AppState {
        config: Arc::new(RwLock::new(cfg)),
        config_path,
        auth: auth_data,
        http: Client::builder().timeout(Duration::from_secs(7)).build().context("Не удалось создать HTTP-клиент")?,
        runtime: Arc::new(RwLock::new(state::RuntimeStatus { voice_monitor_state: "запуск".into(), devlog_state: "запуск".into(), pulse_state: "ожидание PulseAudio".into(), music_player_state: "запуск".into(), ..Default::default() })),
        music_tx: music_tx.clone(),
    };

    voice::spawn_monitor(state.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    devlog::spawn(state.clone(), shutdown_rx.clone());
    pulse::spawn(state.clone(), shutdown_rx.clone());
    yandex_music::spawn_workers(shutdown_rx.clone());
    player::spawn(state.clone(), music_tx, music_rx, shutdown_rx.clone());
    airplay::spawn(state.clone(), shutdown_rx.clone());
    voice_agent_lapi::spawn_lapi_supervisor();

    let app = Router::new()
        .route("/", get(api::index))
        .route("/api/health", get(api::health))
        .route("/api/settings", get(api::get_settings).post(api::save_settings))
        .route("/api/wol/wake", post(api::wake))
        .route("/api/music", post(api::music_command))
        .route("/api/music/status", get(api::music_status))
        .route("/api/music/yandex/probe", post(api::yandex_api_probe))
        .route("/api/pc", get(api::pc_status))
        .route("/api/pc/processes", get(api::pc_processes))
        .route("/api/pc/launch", post(api::pc_launch))
        .route("/api/pc/power", post(api::pc_power))
        .route("/api/logs", get(api::logs))
        .route("/api/led", get(api::get_led).post(api::set_led))
        .route("/api/led/emotion", post(api::led_emotion))
        .route("/api/speech/say", post(api::speech_say))
        .route("/api/speech/phrases", get(api::speech_phrases))
        .route("/api/speech/play", post(api::speech_play))
        .route("/api/llm/ask", post(api::llm_ask))
        .layer(middleware::from_fn_with_state(state.clone(), auth::require_auth))
        .route("/api/internal/music/pause", post(api::internal_pause))
        .with_state(state.clone());

    let address = env::var("CAPSULA_BIND").unwrap_or_else(|_| "0.0.0.0:8770".into());
    let listener = TcpListener::bind(&address).await.context("Не удалось открыть веб-порт")?;
    tracing::info!("Capsula Hub запущен на {address}");
    tracing::info!("Rust DevLogReceiver, VoiceEventParser, PulseStreamTracker, Yandex API/queue и Rust PlaybackSupervisor запущены.");
    tracing::info!("LED arbiter запущен: кольцо через /dev/topleds, экран через clock-led* sysfs.");

    let signal_tx = shutdown_tx.clone();
    axum::serve(listener, app).with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = signal_tx.send(true);
    }).await?;
    let _ = shutdown_tx.send(true);
    ring_blank::end();
    led_arbiter::shutdown();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async { let _ = tokio::signal::ctrl_c().await; };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
