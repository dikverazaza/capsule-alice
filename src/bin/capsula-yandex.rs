use anyhow::{Context, Result};
use capsula_lib::{
    auth::AuthData,
    config::AppConfig,
    devlog, player::{self, PlayerCommand}, pulse,
    state::{AppState, RuntimeStatus},
    yandex_music,
};
use std::{
    env, fs,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, watch, RwLock};
use tracing_subscriber::EnvFilter;

const VERSION: &str = "1.0.0";

#[tokio::main]
async fn main() -> Result<()> {
    // Explicitly set runtime directory so devlog, pulse, and player use /run/capsula-yandex
    env::set_var("CAPSULA_RUN_DIR", "/run/capsula-yandex");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        println!("capsula-yandex {VERSION} (Голосовой сервис Яндекс Музыки для VK Капсулы Мини)");
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--check-credentials") {
        yandex_music::validate_credentials_file()?;
        println!("credentials=ok");
        return Ok(());
    }
    if let Some(pos) = args.iter().position(|a| a == "--control") {
        if let Some(cmd) = args.get(pos + 1) {
            let fifo = "/run/capsula-yandex/control";
            fs::write(fifo, format!("{cmd}\n"))
                .with_context(|| format!("Не удалось отправить команду в {fifo}"))?;
            println!("Команда отправлена: {cmd}");
            return Ok(());
        }
    }

    let run_dir = PathBuf::from("/run/capsula-yandex");
    let _ = fs::create_dir_all(&run_dir);

    // Ensure credentials exist or warn
    if let Err(e) = yandex_music::validate_credentials_file() {
        tracing::warn!("Внимание: {e:#}");
    } else {
        tracing::info!("Токен Яндекс Музыки успешно загружен.");
    }

    let (music_tx, music_rx) = mpsc::channel(128);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut cfg = AppConfig::default();
    cfg.voice_enabled = true;
    cfg.voice_log_path = "/tmp/capsula-syslog.log".into();
    cfg.yandex_fifo = "/run/capsula-yandex/control".into();

    let state = AppState {
        config: Arc::new(RwLock::new(cfg)),
        config_path: PathBuf::from("/data/capsula-yandex/config.json"),
        auth: AuthData {
            salt: String::new(),
            hash: String::new(),
        },
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(7))
            .build()
            .context("Не удалось создать HTTP клиент")?,
        runtime: Arc::new(RwLock::new(RuntimeStatus {
            voice_monitor_state: "запуск".into(),
            devlog_state: "запуск".into(),
            pulse_state: "ожидание PulseAudio".into(),
            music_player_state: "запуск".into(),
            ..Default::default()
        })),
        music_tx: music_tx.clone(),
    };

    tracing::info!("Запуск сервисов Яндекс Музыки (только голосовое управление)...");
    devlog::spawn(state.clone(), shutdown_rx.clone());
    pulse::spawn(state.clone(), shutdown_rx.clone());
    yandex_music::spawn_workers(shutdown_rx.clone());
    player::spawn(state.clone(), music_tx.clone(), music_rx, shutdown_rx.clone());

    // Create and listen to control FIFO for Alice daemon and CLI
    let fifo_path = "/run/capsula-yandex/control".to_string();
    if let Some(parent) = std::path::Path::new(&fifo_path).parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::remove_file(&fifo_path);
    unsafe {
        let c_path = std::ffi::CString::new(fifo_path.as_str()).unwrap();
        libc::mkfifo(c_path.as_ptr(), 0o666);
    }

    let fifo_tx = music_tx.clone();
    let fifo_shutdown = shutdown_rx.clone();
    let fifo_str = fifo_path.clone();
    std::thread::Builder::new()
        .name("fifo-ctl".into())
        .spawn(move || {
            loop {
                if *fifo_shutdown.borrow() { break; }
                let f = match std::fs::OpenOptions::new().read(true).write(true).open(&fifo_str) {
                    Ok(file) => file,
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(500));
                        continue;
                    }
                };
                let mut reader = std::io::BufReader::new(f);
                let mut line = String::new();
                loop {
                    if *fifo_shutdown.borrow() { break; }
                    line.clear();
                    match std::io::BufRead::read_line(&mut reader, &mut line) {
                        Ok(0) => {
                            std::thread::sleep(Duration::from_millis(100));
                            break;
                        }
                        Ok(_) => {
                            let cmd_str = line.trim();
                            if !cmd_str.is_empty() {
                                tracing::info!("[FIFO] Получена команда: «{}»", cmd_str);
                                if let Some(cmd) = PlayerCommand::from_ui(cmd_str) {
                                    if let Err(e) = fifo_tx.blocking_send(cmd) {
                                        tracing::error!("[FIFO] Ошибка отправки команды: {e}");
                                    } else {
                                        tracing::info!("[FIFO] Команда передана в движок плеера");
                                    }
                                } else {
                                    tracing::warn!("[FIFO] Неизвестная команда: «{}»", cmd_str);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[FIFO] Ошибка чтения FIFO: {e}");
                            break;
                        }
                    }
                }
            }
        })
        .expect("failed to spawn fifo thread");

    tracing::info!("Capsula Yandex Music готова к голосовым командам:");
    tracing::info!("  • «Маруся, включи мою волну»");
    tracing::info!("  • «Маруся, включи мои любимые треки»");
    tracing::info!("  • «Маруся, включи песню <название>»");
    tracing::info!("  • «Маруся, включи мой плейлист»");
    tracing::info!("  • «Маруся, следующий / предыдущий трек»");
    tracing::info!("  • «Маруся, пауза / продолжи / выключи музыку»");

    shutdown_signal().await;
    tracing::info!("Получен сигнал завершения, останавливаем Capsula Yandex Music...");
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(Duration::from_millis(300)).await;
    tracing::info!("Capsula Yandex Music остановлена.");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {}
    }
}
