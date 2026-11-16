//! AirPlay receiver supervisor for Capsula Mini.
//! Supervises the independent `capsula-airplay` daemon.

use crate::state::AppState;
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::sleep;

const AIRPLAY_BIN_CANDIDATES: [&str; 2] = [
    "/data/capsula-hub/capsula-airplay",
    "./capsula-airplay",
];

pub fn spawn(state: AppState, mut shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let bin_path = AIRPLAY_BIN_CANDIDATES.iter()
            .find(|p| Path::new(p).exists())
            .copied();

        let Some(bin) = bin_path else {
            state.runtime.write().await.airplay_state = "не установлен".into();
            tracing::info!("[airplay] capsula-airplay не найден в /data/capsula-hub; AirPlay отключён");
            return;
        };

        tracing::info!("[airplay] Найден бинарник AirPlay: {bin}; запускаю супервизор");
        state.runtime.write().await.airplay_state = "запуск".into();

        loop {
            if *shutdown.borrow() {
                break;
            }

            let mut child = match Command::new(bin)
                .kill_on_drop(true)
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("[airplay] Не удалось запустить {bin}: {e}");
                    state.runtime.write().await.airplay_state = format!("ошибка: {e}");
                    tokio::select! {
                        _ = sleep(Duration::from_secs(10)) => continue,
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { break; }
                            continue;
                        }
                    }
                }
            };

            let pid = child.id().unwrap_or(0);
            tracing::info!("[airplay] capsula-airplay запущен (PID {pid})");
            state.runtime.write().await.airplay_state = "активен".into();

            tokio::select! {
                status = child.wait() => {
                    tracing::warn!("[airplay] capsula-airplay завершился: {status:?}; перезапуск через 5 сек");
                    state.runtime.write().await.airplay_state = "перезапуск".into();
                    sleep(Duration::from_secs(5)).await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = child.kill().await;
                        break;
                    }
                }
            }
        }

        state.runtime.write().await.airplay_state = "остановлен".into();
        tracing::info!("[airplay] Супервизор AirPlay остановлен");
    });
}
