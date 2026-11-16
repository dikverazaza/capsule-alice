use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, net::Ipv4Addr, path::Path, str::FromStr};

pub const CONFIG_PATH: &str = "/data/capsula-hub/config.json";
pub const LEGACY_WOL_CONFIG: &str = "/data/capsula-wol/config.json";
pub const AUTH_PATH: &str = "/data/capsula-wol/web-auth.json";
pub const DEFAULT_YANDEX_FIFO: &str = "/run/capsula-yandex/control";
pub const DEFAULT_VOICE_LOG: &str = "/tmp/capsula-syslog.log";

pub fn run_dir() -> &'static str {
    static DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        std::env::var("CAPSULA_RUN_DIR").unwrap_or_else(|_| {
            if std::path::Path::new("/run/capsula-yandex").exists() {
                "/run/capsula-yandex".to_string()
            } else {
                "/run/capsula-hub".to_string()
            }
        })
    })
}

pub fn voice_relay_path() -> String {
    format!("{}/voice-relay.sock", run_dir())
}

pub fn pulse_control_path() -> String {
    format!("{}/pulse-control.sock", run_dir())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PcApp {
    pub id: String,
    pub label: String,
    pub path: String,
}

impl Default for PcApp {
    fn default() -> Self {
        Self { id: String::new(), label: String::new(), path: String::new() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub device_name: String,
    pub mac: String,
    pub broadcast: String,
    pub port: u16,
    pub voice_phrases: Vec<String>,
    pub deskctl_url: String,
    pub deskctl_api_key: String,
    /// Legacy config value retained when loading older config.json files.
    /// v0.2.9 no longer uses a FIFO; playback commands go to the in-process Rust supervisor.
    pub yandex_fifo: String,
    pub voice_log_path: String,
    pub voice_enabled: bool,
    // Intentionally hard-disabled. There is no API capable of writing to LEDs.
    pub led_controls_enabled: bool,
    pub apps: Vec<PcApp>,
    pub llm: crate::llm::LlmConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            device_name: "Мой компьютер".into(),
            // Imported from the supplied WOL configuration.
            mac: "345A6055B310".into(),
            broadcast: "255.255.255.255".into(),
            port: 9,
            voice_phrases: vec![
                "включи компьютер".into(),
                "разбуди компьютер".into(),
                "включи мой компьютер".into(),
                "разбуди мой компьютер".into(),
                "запусти комп".into(),
                "включи комп".into(),
                "разбуди комп".into(),
                "включи мой комп".into(),
                "разбуди мой комп".into(),
                "включи пк".into(),
                "разбуди пк".into(),
            ],
            deskctl_url: "http://192.168.1.218:9990".into(),
            deskctl_api_key: String::new(),
            yandex_fifo: DEFAULT_YANDEX_FIFO.into(),
            voice_log_path: DEFAULT_VOICE_LOG.into(),
            voice_enabled: true,
            led_controls_enabled: false,
            apps: vec![
                PcApp { id: "brave".into(), label: "Brave".into(), path: r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe".into() },
                PcApp { id: "telemost".into(), label: "Яндекс Телемост".into(), path: r"C:\Program Files\Yandex\YandexTelemost\YandexTelemost.exe".into() },
                PcApp { id: "roblox".into(), label: "Roblox".into(), path: String::new() },
                PcApp { id: "minecraft".into(), label: "Minecraft Launcher".into(), path: String::new() },
                PcApp { id: "telegram".into(), label: "Telegram".into(), path: String::new() },
                PcApp { id: "discord".into(), label: "Discord".into(), path: String::new() },
                PcApp { id: "workbuddy".into(), label: "WorkBuddy".into(), path: String::new() },
                PcApp { id: "steam".into(), label: "Steam".into(), path: r"C:\Program Files (x86)\Steam\steam.exe".into() },
                PcApp { id: "notepad".into(), label: "Блокнот".into(), path: "notepad.exe".into() },
            ],
            llm: crate::llm::LlmConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let raw = fs::read(path).with_context(|| format!("Не удалось прочитать {}", path.display()))?;
            let mut cfg: Self = serde_json::from_slice(&raw).context("Некорректный config.json")?;
            #[cfg(unix)] {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
            }
            cfg.led_controls_enabled = false;
            cfg.validate()?;
            return Ok(cfg);
        }

        let mut cfg = Self::default();
        // Migrate the old WOL panel settings once, without deleting the old file.
        if let Ok(raw) = fs::read(LEGACY_WOL_CONFIG) {
            if let Ok(old) = serde_json::from_slice::<serde_json::Value>(&raw) {
                if let Some(v) = old.get("device_name").and_then(|v| v.as_str()) { cfg.device_name = v.to_owned(); }
                if let Some(v) = old.get("mac").and_then(|v| v.as_str()) { cfg.mac = v.to_owned(); }
                if let Some(v) = old.get("broadcast").and_then(|v| v.as_str()) { cfg.broadcast = v.to_owned(); }
                if let Some(v) = old.get("port").and_then(|v| v.as_u64()) { cfg.port = v as u16; }
                if let Some(items) = old.get("voice_phrases").and_then(|v| v.as_array()) {
                    let phrases: Vec<String> = items.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect();
                    if !phrases.is_empty() { cfg.voice_phrases = phrases; }
                }
            }
        }
        cfg.validate()?;
        save(path, &cfg)?;
        Ok(cfg)
    }

    pub fn validate(&mut self) -> Result<()> {
        self.device_name = self.device_name.split_whitespace().collect::<Vec<_>>().join(" ");
        if self.device_name.is_empty() { self.device_name = "Мой компьютер".into(); }
        self.device_name = self.device_name.chars().take(48).collect();

        let mac: String = self.mac.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        if !mac.is_empty() && mac.len() != 12 {
            return Err(anyhow!("MAC-адрес должен содержать 12 шестнадцатеричных символов"));
        }
        self.mac = mac.to_ascii_uppercase();

        self.broadcast = Ipv4Addr::from_str(self.broadcast.trim())
            .map_err(|_| anyhow!("Broadcast должен быть корректным IPv4-адресом"))?.to_string();
        if !(1..=65535).contains(&self.port) { return Err(anyhow!("UDP-порт должен быть в диапазоне 1–65535")); }

        self.deskctl_url = self.deskctl_url.trim().trim_end_matches('/').to_owned();
        if !self.deskctl_url.is_empty() && !(self.deskctl_url.starts_with("http://") || self.deskctl_url.starts_with("https://")) {
            return Err(anyhow!("Адрес Deskctl должен начинаться с http:// или https://"));
        }
        self.yandex_fifo = self.yandex_fifo.trim().to_owned();
        self.voice_log_path = self.voice_log_path.trim().to_owned();
        if self.yandex_fifo.is_empty() { self.yandex_fifo = DEFAULT_YANDEX_FIFO.into(); }
        if self.voice_log_path.is_empty() { self.voice_log_path = DEFAULT_VOICE_LOG.into(); }

        let mut phrases = Vec::new();
        for phrase in self.voice_phrases.drain(..) {
            let value = phrase.split_whitespace().collect::<Vec<_>>().join(" ");
            if !value.is_empty() && !phrases.iter().any(|p: &String| p.eq_ignore_ascii_case(&value)) {
                phrases.push(value.chars().take(64).collect());
            }
        }
        if phrases.is_empty() { return Err(anyhow!("Добавьте хотя бы одну голосовую фразу WOL")); }
        if phrases.len() > 12 { return Err(anyhow!("Можно задать не более 12 голосовых фраз")); }
        self.voice_phrases = phrases;

        if self.apps.len() > 32 { return Err(anyhow!("Слишком много приложений (максимум 32)")); }
        let mut ids = Vec::new();
        for app in &mut self.apps {
            app.id = app.id.trim().to_ascii_lowercase();
            if app.id.is_empty() || !app.id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
                return Err(anyhow!("ID приложения может содержать только латинские буквы, цифры, - и _"));
            }
            if ids.contains(&app.id) { return Err(anyhow!("ID приложения должен быть уникальным: {}", app.id)); }
            ids.push(app.id.clone());
            app.label = app.label.trim().chars().take(48).collect();
            app.path = app.path.trim().chars().take(512).collect();
            if app.label.is_empty() { return Err(anyhow!("Укажите название приложения для {}", app.id)); }
        }

        // This remains false even if an old config file contains true.
        self.led_controls_enabled = false;
        self.llm.validate()?;
        Ok(())
    }
}

pub fn save(path: &Path, config: &AppConfig) -> Result<()> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let tmp = path.with_extension("json.new");
    fs::write(&tmp, serde_json::to_vec_pretty(config)?)?;
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(&tmp, path).with_context(|| format!("Не удалось сохранить {}", path.display()))?;
    Ok(())
}
