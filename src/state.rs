use crate::{auth::AuthData, config::AppConfig};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{mpsc, RwLock};
use crate::player::PlayerCommand;

#[derive(Clone, Default)]
pub struct RuntimeStatus {
    pub last_voice: String,
    pub last_wake: String,
    pub last_error: String,
    pub voice_monitor_state: String,
    pub devlog_state: String,
    pub pulse_state: String,
    pub wake_count: u64,
    pub music_player_state: String,
    pub airplay_state: String,
    pub llm_dialog_pending_until: Option<tokio::time::Instant>,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<AppConfig>>,
    pub config_path: PathBuf,
    pub auth: AuthData,
    pub http: reqwest::Client,
    pub runtime: Arc<RwLock<RuntimeStatus>>,
    pub music_tx: mpsc::Sender<PlayerCommand>,
}
