//! Rust-owned Yandex Music API bridge.
//!
//! Authentication, liked-track loading, Rotor sessions/feedback, signed MP3
//! URL resolution, and playback queue control run in-process in the Rust Hub.
//! Signed URLs are passed only to the local decoder process and are never
//! written to logs or persistent state.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::{header, Client, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex},
    task::JoinSet,
    time::timeout,
};

const API_BASE: &str = "https://api.music.yandex.net";
const CREDENTIAL_PATHS: [&str; 4] = [
    "/etc/alice/yandex_credentials.json",
    "/data/capsula-hub/credentials.json",
    "/data/capsula-yandex/credentials.json",
    "/opt/etc/capsula-yandex/credentials.json",
];
const MAX_CREDENTIAL_BYTES: u64 = 16 * 1024;
const MAX_API_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const INITIAL_BODY_RESERVE: u64 = 64 * 1024;

fn initial_body_capacity(content_length: Option<u64>, max_bytes: usize) -> usize {
    content_length.unwrap_or(0)
        .min(max_bytes as u64)
        .min(INITIAL_BODY_RESERVE) as usize
}
const MAX_XML_BYTES: usize = 64 * 1024;
const MAX_TRACKS_PER_QUEUE: usize = 5000;
const LIKED_TRACK_BATCH: usize = 50;
const USER_AGENT: &str = "CapsulaHub/0.2.9 (Linux; aarch64)";
const YANDEX_MUSIC_CLIENT: &str = "YandexMusicAndroid/24023621";
const FEEDBACK_QUEUE_CAPACITY: usize = 64;
static HTTP_CLIENT: OnceLock<std::sync::Mutex<Option<Client>>> = OnceLock::new();

fn shared_http_client() -> Result<Client> {
    let cache = HTTP_CLIENT.get_or_init(|| std::sync::Mutex::new(None));
    let mut cached = cache.lock().map_err(|_| anyhow!("HTTP client cache poisoned"))?;
    if cached.is_none() {
        *cached = Some(
            Client::builder()
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(25))
                .redirect(reqwest::redirect::Policy::limited(5))
                .build()
                .context("Не удалось создать HTTP-клиент Яндекс Музыки")?,
        );
    }
    cached.as_ref().cloned().ok_or_else(|| anyhow!("HTTP client cache is empty"))
}
const DOWNLOAD_SIGN_KEY: &str = "XGRlBW9FXlekgbPrRHuSiA";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MusicTrack {
    pub id: String,
    pub queue_id: String,
    pub title: String,
    pub artist: String,
    pub duration_ms: Option<u64>,
    pub album_id: Option<String>,
    pub cover_uri: Option<String>,
    #[serde(default)]
    pub normalization_gain: Option<f32>,
}

#[derive(Clone, Debug)]
struct LikedTrackRef {
    id: String,
    album_id: Option<String>,
}

#[derive(Debug)]
struct RotorBatch {
    batch_id: Option<String>,
    tracks: Vec<MusicTrack>,
}

#[derive(Clone)]
pub struct YandexApi {
    pub token: String,
    pub http: Client,
}

#[derive(Clone, Debug)]
struct QueuedTrack {
    track: MusicTrack,
    batch_id: Option<String>,
}

#[derive(Clone, Debug)]
struct ActiveTrack {
    ticket: u64,
    queued: QueuedTrack,
    started: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaylistSummary {
    pub owner_uid: String,
    pub kind: u64,
    pub title: String,
    pub track_count: u64,
    pub cover_uri: Option<String>,
}

#[derive(Default)]
struct QueueEngine {
    mode: String,
    track_query: Option<String>,
    playlist_query: Option<String>,
    playlist_title: Option<String>,
    liked_tracks: Vec<MusicTrack>,
    liked_index: usize,
    playlist_tracks: Vec<MusicTrack>,
    playlist_index: usize,
    search_tracks: Vec<MusicTrack>,
    search_index: usize,
    wave_session_id: Option<String>,
    wave_pending: VecDeque<QueuedTrack>,
    wave_history: VecDeque<QueuedTrack>,
    wave_played_ids: VecDeque<String>,
    current: Option<ActiveTrack>,
    next_ticket: u64,
    consecutive_errors: usize,
}

impl QueueEngine {
    fn reset(&mut self, mode: &str) {
        let next_ticket = self.next_ticket;
        *self = Self {
            mode: mode.to_owned(),
            next_ticket,
            ..Self::default()
        };
    }

    fn release_payload(&mut self) {
        let next_ticket = self.next_ticket;
        *self = Self {
            mode: "idle".to_owned(),
            next_ticket,
            ..Self::default()
        };
    }
}

static QUEUE_ENGINE: OnceLock<Mutex<QueueEngine>> = OnceLock::new();

fn queue_engine() -> &'static Mutex<QueueEngine> {
    QUEUE_ENGINE.get_or_init(|| Mutex::new(QueueEngine::default()))
}

enum FeedbackJob {
    Event {
        session_id: String,
        event: String,
        track_id: Option<String>,
        batch_id: Option<String>,
        seconds: Option<f64>,
    },
    Barrier(oneshot::Sender<()>),
}

static FEEDBACK_SENDER: OnceLock<mpsc::Sender<FeedbackJob>> = OnceLock::new();

fn try_send_feedback(sender: &mpsc::Sender<FeedbackJob>, job: FeedbackJob, event: &str) -> Result<()> {
    match sender.try_send(job) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!("[rotor] Очередь feedback заполнена ({FEEDBACK_QUEUE_CAPACITY}); событие {event} отброшено, playback не блокируется");
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("Rotor feedback worker остановлен")),
    }
}

fn enqueue_feedback(
    session_id: &str,
    event: &str,
    track_id: Option<&str>,
    batch_id: Option<&str>,
    seconds: Option<f64>,
) -> Result<()> {
    let sender = FEEDBACK_SENDER.get().ok_or_else(|| anyhow!("Rotor feedback worker ещё не запущен"))?;
    try_send_feedback(sender, FeedbackJob::Event {
        session_id: session_id.to_owned(),
        event: event.to_owned(),
        track_id: track_id.map(str::to_owned),
        batch_id: batch_id.map(str::to_owned),
        seconds,
    }, event)
}

async fn flush_feedback() -> Result<()> {
    let Some(sender) = FEEDBACK_SENDER.get() else {
        return Ok(());
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    match timeout(Duration::from_millis(500), sender.send(FeedbackJob::Barrier(reply_tx))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Ok(()),
        Err(_) => {
            tracing::warn!("[rotor] Не удалось поставить barrier в очередь за 500 мс; продолжаю без ожидания");
            return Ok(());
        }
    }
    if timeout(Duration::from_secs(2), reply_rx).await.is_err() {
        tracing::warn!("[rotor] Feedback не завершился за 2 с; продолжаю переход очереди");
    }
    Ok(())
}

async fn run_feedback_worker(
    mut receiver: mpsc::Receiver<FeedbackJob>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let job = tokio::select! {
            received = receiver.recv() => match received {
                Some(job) => job,
                None => break,
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
                continue;
            }
        };
        match job {
            FeedbackJob::Event { session_id, event, track_id, batch_id, seconds } => {
                let result = match YandexApi::new() {
                    Ok(api) => api.send_feedback(
                        &session_id,
                        &event,
                        track_id.as_deref(),
                        batch_id.as_deref(),
                        seconds,
                    ).await,
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    tracing::warn!("[rotor] {event} не подтверждён: {}", safe_error(&error.to_string()));
                }
            }
            FeedbackJob::Barrier(reply) => {
                let _ = reply.send(());
            }
        }
    }
    tracing::warn!("[rotor] Feedback worker завершился");
}

impl YandexApi {
    pub fn new() -> Result<Self> {
        let token = read_token()?;
        let http = shared_http_client()?;
        Ok(Self { token, http })
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let response = self.http
            .get(format!("{API_BASE}/{path}"))
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .context("Не удалось связаться с API Яндекс Музыки")?;
        read_api_response(response).await
    }

    async fn get_json_query(&self, path: &str, key: &str, value: &str) -> Result<Value> {
        let response = self.http
            .get(format!("{API_BASE}/{path}"))
            .query(&[(key, value)])
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .context("Не удалось связаться с API Яндекс Музыки")?;
        read_api_response(response).await
    }

    async fn get_json_queries(&self, path: &str, queries: &[(&str, &str)]) -> Result<Value> {
        let response = self.http
            .get(format!("{API_BASE}/{path}"))
            .query(queries)
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .context("Не удалось связаться с API Яндекс Музыки")?;
        read_api_response(response).await
    }

    async fn post_json(&self, path: &str, payload: Value) -> Result<Value> {
        let response = self.http
            .post(format!("{API_BASE}/{path}"))
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .header(header::ACCEPT, "application/json")
            .json(&payload)
            .send()
            .await
            .context("Не удалось связаться с API Яндекс Музыки")?;
        read_api_response(response).await
    }

    async fn account_uid(&self) -> Result<String> {
        let value = self.get_json("account/status").await?;
        let account = result_object(&value)
            .get("account")
            .filter(|account| account.is_object())
            .ok_or_else(|| anyhow!("Яндекс не вернул данные авторизованного аккаунта"))?;
        let uid = string_field(account, &["uid", "userId", "user_id"])
            .ok_or_else(|| anyhow!("В ответе Яндекса отсутствует ID аккаунта"))?;
        validate_numeric_id(&uid, "ID аккаунта")?;
        Ok(uid)
    }

    async fn liked_tracks(&self) -> Result<Vec<MusicTrack>> {
        let uid = self.account_uid().await?;
        let refs = {
            let value = self.get_json(&format!("users/{uid}/likes/tracks")).await?;
            parse_liked_refs(&value)
        };
        if refs.is_empty() {
            bail!("Яндекс не вернул треки из Избранного");
        }
        if refs.len() > MAX_TRACKS_PER_QUEUE {
            bail!("Список Избранного превышает лимит {MAX_TRACKS_PER_QUEUE} треков");
        }

        // Keep only compact MusicTrack records. Full JSON objects are parsed
        // and dropped per response batch instead of accumulating 5000 Values.
        let mut indices_by_id: HashMap<&str, Vec<usize>> = HashMap::with_capacity(refs.len());
        for (index, reference) in refs.iter().enumerate() {
            indices_by_id.entry(reference.id.as_str()).or_default().push(index);
        }
        let mut ordered_tracks: Vec<Option<MusicTrack>> = vec![None; refs.len()];
        for group in refs.chunks(LIKED_TRACK_BATCH * LIKED_TRACK_FETCH_CONCURRENCY) {
            let mut pending = JoinSet::new();
            for batch in group.chunks(LIKED_TRACK_BATCH) {
                let ids = batch.iter().map(|item| item.id.as_str()).collect::<Vec<_>>().join(",");
                let api = self.clone();
                pending.spawn(async move {
                    let response = api.get_json_query("tracks", "track-ids", &ids).await?;
                    let tracks = parse_track_array(&response).into_iter().cloned().collect::<Vec<_>>();
                    Ok::<Vec<Value>, anyhow::Error>(tracks)
                });
            }
            while let Some(joined) = pending.join_next().await {
                for full in joined.context("Задача загрузки метаданных Избранного завершилась аварийно")?? {
                    let Some(id) = string_field(&full, &["id", "realId", "real_id"]) else { continue; };
                    if let Some(indices) = indices_by_id.get(id.as_str()) {
                        for index in indices {
                            if let Some(track) = parse_music_track(&full, refs[*index].album_id.as_deref()) {
                                ordered_tracks[*index] = Some(track);
                            }
                        }
                    }
                }
            }
        }

        let tracks = ordered_tracks.into_iter().flatten().collect::<Vec<_>>();
        drop(indices_by_id);
        drop(refs);
        if tracks.is_empty() {
            bail!("Не удалось получить метаданные треков Избранного");
        }
        Ok(tracks)
    }

    async fn wave_start(&self) -> Result<Value> {
        let value = self.post_json(
            "rotor/session/new",
            json!({
                "seeds": ["user:onyourwave"],
                "includeTracksInResponse": true
            }),
        ).await?;
        let session = result_object(&value);
        let session_id = string_field(session, &["radioSessionId", "radio_session_id"])
            .ok_or_else(|| anyhow!("Яндекс не вернул ID сессии Моей волны"))?;
        validate_session_id(&session_id)?;
        let batch = parse_rotor_batch(session);
        if batch.tracks.is_empty() {
            bail!("Первая подборка Моей волны оказалась пустой");
        }

        // Rotor should receive exactly one radioStarted event for a new
        // playback session. A feedback failure is non-fatal to playback.
        let feedback_ok = self
            .send_feedback(&session_id, "radio_started", None, batch.batch_id.as_deref(), None)
            .await
            .is_ok();

        Ok(json!({
            "ok": true,
            "session_id": session_id,
            "batch_id": batch.batch_id,
            "tracks": batch.tracks,
            "radio_started_feedback": feedback_ok
        }))
    }

    async fn wave_next(&self, session_id: &str, queue: &[String]) -> Result<Value> {
        validate_session_id(session_id)?;
        if queue.len() > MAX_TRACKS_PER_QUEUE {
            bail!("Очередь Моей волны слишком большая");
        }
        let mut safe_queue = Vec::with_capacity(queue.len());
        for id in queue {
            validate_queue_id(id)?;
            safe_queue.push(id.clone());
        }
        let value = self.post_json(
            &format!("rotor/session/{session_id}/tracks"),
            json!({ "queue": safe_queue, "feedbacks": [] }),
        ).await?;
        let batch = parse_rotor_batch(result_object(&value));
        if batch.tracks.is_empty() {
            bail!("Яндекс не вернул следующую подборку Моей волны");
        }
        Ok(json!({
            "ok": true,
            "batch_id": batch.batch_id,
            "tracks": batch.tracks
        }))
    }

    async fn send_feedback(
        &self,
        session_id: &str,
        event: &str,
        track_id: Option<&str>,
        batch_id: Option<&str>,
        seconds: Option<f64>,
    ) -> Result<()> {
        validate_session_id(session_id)?;
        let payload = build_feedback_payload(event, track_id, batch_id, seconds)?;
        let response = self.http
            .post(format!("{API_BASE}/rotor/session/{session_id}/feedback"))
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .header(header::ACCEPT, "application/json")
            .json(&payload)
            .send()
            .await
            .context("Не удалось отправить Rotor feedback")?;
        let status = response.status();
        let body = read_limited_body(response, 64 * 1024)
            .await
            .context("Не удалось прочитать ответ Rotor feedback")?;
        validate_feedback_response(status, &body)
    }

    async fn resolve_audio(&self, track_id: &str) -> Result<Value> {
        validate_track_id(track_id, "ID трека")?;
        let value = self
            .get_json_query(&format!("tracks/{track_id}/download-info"), "can_use_streaming", "true")
            .await?;
        let variants = result_array(&value)
            .ok_or_else(|| anyhow!("Яндекс не вернул варианты аудиопотока"))?;

        let mut choices: Vec<(&Value, u64)> = variants.iter().filter_map(|item| {
            let codec = item.get("codec").and_then(Value::as_str).unwrap_or("");
            let preview = item.get("preview").and_then(Value::as_bool).unwrap_or(false);
            let bitrate = item.get("bitrateInKbps")
                .or_else(|| item.get("bitrate_in_kbps"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if codec.eq_ignore_ascii_case("mp3") && !preview {
                Some((item, bitrate))
            } else {
                None
            }
        }).collect();
        choices.sort_by_key(|(_, bitrate)| *bitrate);
        let (selected, bitrate) = choices.pop()
            .ok_or_else(|| anyhow!("Нет доступного полноразмерного MP3-потока; проверьте подписку и доступность трека"))?;
        let download_info_url = string_field(selected, &["downloadInfoUrl", "download_info_url"])
            .ok_or_else(|| anyhow!("Вариант MP3 не содержит downloadInfoUrl"))?;
        let direct = selected.get("direct").and_then(Value::as_bool).unwrap_or(false);
        let stream_url = if direct {
            validate_yandex_url(&download_info_url)?;
            download_info_url
        } else {
            self.resolve_xml_download_url(&download_info_url).await?
        };
        Ok(json!({
            "ok": true,
            "url": stream_url,
            "bitrate_kbps": bitrate
        }))
    }

    /// Полноценный поиск треков по запросу через API Яндекс Музыки.
    pub async fn search_tracks(&self, query: &str) -> Result<Vec<MusicTrack>> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            bail!("Поисковый запрос трека пустой");
        }
        let value = self
            .get_json_queries("search", &[("text", trimmed), ("type", "track"), ("page", "0")])
            .await
            .context("Не удалось выполнить поиск трека в Яндекс Музыке")?;
        let tracks_obj = result_object(&value).get("tracks");
        let items = tracks_obj
            .and_then(|t| t.get("results"))
            .and_then(Value::as_array)
            .or_else(|| tracks_obj.and_then(Value::as_array))
            .ok_or_else(|| anyhow!("Яндекс не вернул результаты поиска"))?;

        let mut tracks = Vec::new();
        for item in items {
            let track_obj = item.get("track").filter(|t| t.is_object()).unwrap_or(item);
            if let Some(music_track) = parse_music_track(track_obj, None) {
                tracks.push(music_track);
            }
        }
        if tracks.is_empty() {
            bail!("По запросу «{}» не найдено подходящих треков", bounded(trimmed, 80));
        }
        Ok(tracks)
    }

    /// Поставить лайк треку в Яндекс Музыке (добавить в Избранное).
    pub async fn like_track(&self, track_id: &str) -> Result<()> {
        let uid = self.account_uid().await?;
        let form = [("track-ids", track_id)];
        let response = self.http
            .post(format!("{API_BASE}/users/{uid}/likes/tracks/add-multiple"))
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .form(&form)
            .send()
            .await
            .context("Не удалось отправить like в Яндекс Музыку")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Ошибка добавления лайка: HTTP {status} — {body}");
        }
        tracing::info!("[YandexMusic] Трек {track_id} успешно добавлен в Избранное (лайк)");
        Ok(())
    }

    /// Поставить дизлайк треку в Яндекс Музыке (скрыть из рекомендаций).
    pub async fn dislike_track(&self, track_id: &str) -> Result<()> {
        let uid = self.account_uid().await?;
        let form = [("track-ids", track_id)];
        let response = self.http
            .post(format!("{API_BASE}/users/{uid}/dislikes/tracks/add-multiple"))
            .header(header::AUTHORIZATION, format!("OAuth {}", self.token))
            .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
            .form(&form)
            .send()
            .await
            .context("Не удалось отправить dislike в Яндекс Музыку")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Ошибка добавления дизлайка: HTTP {status} — {body}");
        }
        tracing::info!("[YandexMusic] Трек {track_id} добавлен в черный список (дизлайк)");
        Ok(())
    }

    /// Получение списка пользовательских плейлистов.
    pub async fn user_playlists(&self) -> Result<Vec<PlaylistSummary>> {
        let uid = self.account_uid().await?;
        let value = self.get_json(&format!("users/{uid}/playlists/list")).await?;
        let items = result_array(&value)
            .ok_or_else(|| anyhow!("Яндекс не вернул список плейлистов"))?;
        let mut playlists = Vec::new();
        for item in items {
            let kind = item.get("kind").and_then(Value::as_u64);
            let title = item.get("title").and_then(Value::as_str).unwrap_or("Плейлист").to_owned();
            let track_count = item.get("trackCount").or_else(|| item.get("track_count")).and_then(Value::as_u64).unwrap_or(0);
            let owner_uid = item.get("owner").and_then(|o| string_field(o, &["uid"])).unwrap_or_else(|| uid.clone());
            let cover_uri = item.get("cover").and_then(|c| c.get("uri"))
                .or_else(|| item.get("ogImage"))
                .and_then(Value::as_str)
                .map(|v| bounded(v, 256));
            if let Some(k) = kind {
                playlists.push(PlaylistSummary { owner_uid, kind: k, title, track_count, cover_uri });
            }
        }
        Ok(playlists)
    }

    /// Поиск плейлистов по названию.
    pub async fn search_playlists(&self, query: &str) -> Result<Vec<PlaylistSummary>> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let value = self
            .get_json_queries("search", &[("text", trimmed), ("type", "playlist"), ("page", "0")])
            .await
            .context("Не удалось выполнить поиск плейлистов в Яндекс Музыке")?;
        let pl_obj = result_object(&value).get("playlists");
        let items = pl_obj
            .and_then(|p| p.get("results"))
            .and_then(Value::as_array)
            .or_else(|| pl_obj.and_then(Value::as_array));
        let Some(items) = items else { return Ok(Vec::new()); };

        let mut playlists = Vec::new();
        for item in items {
            let kind = item.get("kind").and_then(Value::as_u64);
            let title = item.get("title").and_then(Value::as_str).unwrap_or("Плейлист").to_owned();
            let track_count = item.get("trackCount").or_else(|| item.get("track_count")).and_then(Value::as_u64).unwrap_or(0);
            let owner_uid = item.get("owner").and_then(|o| string_field(o, &["uid"])).unwrap_or_default();
            let cover_uri = item.get("cover").and_then(|c| c.get("uri"))
                .or_else(|| item.get("ogImage"))
                .and_then(Value::as_str)
                .map(|v| bounded(v, 256));
            if let Some(k) = kind {
                if !owner_uid.is_empty() {
                    playlists.push(PlaylistSummary { owner_uid, kind: k, title, track_count, cover_uri });
                }
            }
        }
        Ok(playlists)
    }

    /// Загрузка треков конкретного плейлиста.
    pub async fn playlist_tracks(&self, owner_uid: &str, kind: u64) -> Result<Vec<MusicTrack>> {
        validate_numeric_id(owner_uid, "UID владельца плейлиста")?;
        let value = self.get_json(&format!("users/{owner_uid}/playlists/{kind}")).await?;
        let result = result_object(&value);
        let items = result.get("tracks").and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Яндекс не вернул треки плейлиста"))?;
        let mut tracks = Vec::new();
        for item in items {
            let track_obj = item.get("track").filter(|t| t.is_object()).unwrap_or(item);
            if let Some(music_track) = parse_music_track(track_obj, None) {
                tracks.push(music_track);
            }
        }
        Ok(tracks)
    }

    async fn resolve_xml_download_url(&self, download_info_url: &str) -> Result<String> {
        validate_yandex_url(download_info_url)?;
        let response = self.http
            .get(download_info_url)
            .header(header::ACCEPT, "application/xml,text/xml")
            .send()
            .await
            .context("Не удалось получить XML-информацию аудиопотока")?;
        let status = response.status();
        if !status.is_success() {
            bail!("Сервер аудиопотока вернул HTTP {}", status.as_u16());
        }
        if response.content_length().map(|size| size > MAX_XML_BYTES as u64).unwrap_or(false) {
            bail!("XML-информация аудиопотока слишком большая");
        }
        let bytes = read_limited_body(response, MAX_XML_BYTES).await
            .context("Не удалось прочитать XML аудиопотока")?;
        let xml = std::str::from_utf8(&bytes).context("XML аудиопотока не является UTF-8")?;
        let host = xml_tag(xml, "host").ok_or_else(|| anyhow!("В XML отсутствует host"))?;
        let path = xml_tag(xml, "path").ok_or_else(|| anyhow!("В XML отсутствует path"))?;
        let timestamp = xml_tag(xml, "ts").ok_or_else(|| anyhow!("В XML отсутствует ts"))?;
        let sign_part = xml_tag(xml, "s").ok_or_else(|| anyhow!("В XML отсутствует s"))?;
        validate_media_host(&host)?;
        if !path.starts_with('/') || path.len() < 2 || path.len() > 2048
            || path.chars().any(|ch| ch.is_control() || ch.is_whitespace())
            || path.contains("..")
        {
            bail!("Яндекс вернул некорректный путь аудиофайла");
        }
        if timestamp.is_empty() || timestamp.len() > 64
            || !timestamp.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        {
            bail!("Яндекс вернул некорректный timestamp аудиофайла");
        }
        if sign_part.is_empty() || sign_part.len() > 512
            || !sign_part.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        {
            bail!("Яндекс вернул некорректную подпись аудиофайла");
        }
        let signature_input = format!("{DOWNLOAD_SIGN_KEY}{}{sign_part}", &path[1..]);
        let signature = md5_hex(signature_input.as_bytes());
        let final_url = format!("https://{host}/get-mp3/{signature}/{timestamp}{path}");
        validate_yandex_url(&final_url)?;
        Ok(final_url)
    }
}

/// Start the ordered Rotor feedback worker used by the in-process Rust queue.
///
/// The former Python-facing Yandex API Unix socket has been removed: the Rust
/// PlaybackSupervisor now calls the queue/API directly, so no signed URL or
/// OAuth-derived response is exposed through a second local control surface.
pub fn spawn_workers(shutdown: watch::Receiver<bool>) {
    let (feedback_tx, feedback_rx) = mpsc::channel(FEEDBACK_QUEUE_CAPACITY);
    match FEEDBACK_SENDER.set(feedback_tx) {
        Ok(()) => {
            tokio::spawn(run_feedback_worker(feedback_rx, shutdown));
        }
        Err(_) => {
            tracing::error!("[rotor] Feedback worker уже был запущен; второй экземпляр не создаётся");
        }
    }
}

const MAX_QUEUE_ERRORS: usize = 8;
const MAX_WAVE_QUEUE_IDS: usize = 200;
const MAX_WAVE_HISTORY: usize = 100;
const LIKED_TRACK_FETCH_CONCURRENCY: usize = 4;

fn valid_queue_outcome(outcome: &str) -> bool {
    matches!(outcome, "finished" | "next" | "previous" | "wave" | "liked" | "stock_radio" | "stop" | "idle" | "error" | "dislike")
        || outcome.starts_with("track:")
        || outcome.starts_with("playlist:")
}

fn remember_wave_track(queue: &mut QueueEngine, track: QueuedTrack) {
    queue.wave_history.push_back(track);
    while queue.wave_history.len() > MAX_WAVE_HISTORY {
        queue.wave_history.pop_front();
    }
}

fn restore_previous_wave_track(queue: &mut QueueEngine, current: QueuedTrack) {
    if let Some(previous) = queue.wave_history.pop_back() {
        queue.wave_pending.push_front(current);
        queue.wave_pending.push_front(previous);
    } else {
        queue.wave_pending.push_front(current);
    }
}

/// In-process playback API used by the Rust-owned player supervisor.
/// Queue control is intentionally in-process; no Python-facing API socket is exposed.
pub async fn player_queue_start(mode: &str, track_query: Option<&str>) -> Result<Value> {
    let api = YandexApi::new()?;
    queue_start(&api, mode, track_query).await
}

pub async fn player_mark_track_started(ticket: u64) -> Result<Value> {
    queue_track_started(ticket).await
}

pub async fn player_queue_advance(ticket: u64, outcome: &str, seconds: f64) -> Result<Value> {
    let api = YandexApi::new()?;
    queue_advance(&api, ticket, outcome, seconds).await
}

pub async fn like_track(track_id: &str) -> Result<()> {
    let api = YandexApi::new()?;
    api.like_track(track_id).await
}

pub async fn dislike_track(track_id: &str) -> Result<()> {
    let api = YandexApi::new()?;
    api.dislike_track(track_id).await
}

async fn queue_start(api: &YandexApi, mode: &str, track_query: Option<&str>) -> Result<Value> {
    if !matches!(mode, "liked" | "wave" | "track" | "playlist") {
        bail!("Режим музыкальной очереди должен быть liked, wave, track или playlist");
    }
    if mode == "wave" {
        flush_feedback().await?;
    }
    let mut queue = queue_engine().lock().await;
    queue.reset(mode);
    match mode {
        "liked" => {
            queue.liked_tracks = api.liked_tracks().await?;
            tracing::info!("[music-queue] Избранное загружено в Rust: {} треков", queue.liked_tracks.len());
        }
        "wave" => {
            let session = api.wave_start().await?;
            let session_id = session.get("session_id").and_then(Value::as_str)
                .ok_or_else(|| anyhow!("Rotor не вернул session_id"))?.to_owned();
            validate_session_id(&session_id)?;
            let batch_id = session.get("batch_id").and_then(Value::as_str).map(str::to_owned);
            let tracks = tracks_from_value(session.get("tracks"))?;
            if tracks.is_empty() {
                bail!("Первая подборка Моей волны пустая");
            }
            queue.wave_session_id = Some(session_id);
            queue.wave_pending.extend(tracks.into_iter().map(|track| QueuedTrack {
                track,
                batch_id: batch_id.clone(),
            }));
            let feedback_ok = session.get("radio_started_feedback").and_then(Value::as_bool).unwrap_or(false);
            if !feedback_ok {
                tracing::warn!("[rotor] Яндекс не подтвердил radioStarted; воспроизведение продолжится");
            }
        }
        "track" => {
            let query = track_query.ok_or_else(|| anyhow!("Для режима track нужен поисковый запрос трека"))?;
            let tracks = if !query.is_empty() && query.chars().all(|c| c.is_ascii_digit()) {
                let response = api.get_json_query("tracks", "track-ids", query).await?;
                parse_track_array(&response).into_iter()
                    .filter_map(|t| parse_music_track(t, None))
                    .collect()
            } else {
                api.search_tracks(query).await?
            };
            if tracks.is_empty() {
                bail!("По запросу «{query}» треков не найдено");
            }
            queue.track_query = Some(query.to_owned());
            queue.search_tracks = tracks;
            queue.search_index = 0;
            tracing::info!("[music-queue] Загружено {} треков по запросу «{}»", queue.search_tracks.len(), query);
        }
        "playlist" => {
            let query_clean = track_query.map(str::trim).filter(|q| !q.is_empty());
            let user_playlists = api.user_playlists().await.unwrap_or_default();
            let selected = if let Some(query) = query_clean {
                if let Ok(kind_id) = query.parse::<u64>() {
                    user_playlists.iter().find(|p| p.kind == kind_id).cloned()
                } else {
                    let lower_query = query.to_lowercase();
                    user_playlists.iter().find(|p| p.title.to_lowercase().contains(&lower_query)).cloned()
                }
            } else {
                user_playlists.into_iter().next()
            };

            let (owner_uid, kind, title) = if let Some(pl) = selected {
                (pl.owner_uid, pl.kind, pl.title)
            } else if let Some(query) = query_clean {
                let searched = api.search_playlists(query).await?;
                let pl = searched.into_iter().next()
                    .ok_or_else(|| anyhow!("Плейлист «{query}» не найден"))?;
                (pl.owner_uid, pl.kind, pl.title)
            } else {
                tracing::info!("[music-queue] Плейлисты не найдены, переключаюсь на Избранное");
                queue.reset("liked");
                queue.liked_tracks = api.liked_tracks().await?;
                return queue_prepare_next(api, &mut queue).await;
            };

            let tracks = api.playlist_tracks(&owner_uid, kind).await?;
            if tracks.is_empty() {
                bail!("Плейлист «{title}» пустой");
            }
            tracing::info!("[music-queue] Загружен плейлист «{}»: {} треков", title, tracks.len());
            queue.playlist_title = Some(title);
            queue.playlist_query = query_clean.map(str::to_owned);
            queue.playlist_tracks = tracks;
            queue.playlist_index = 0;
        }
        _ => unreachable!(),
    }
    queue_prepare_next(api, &mut queue).await
}

async fn queue_track_started(ticket: u64) -> Result<Value> {
    let mut queue = queue_engine().lock().await;
    let mode = queue.mode.clone();
    let (already_started, track_id, batch_id) = {
        let current = queue.current.as_mut()
            .filter(|current| current.ticket == ticket)
            .ok_or_else(|| anyhow!("Трек уже сменился; ticket устарел"))?;
        (
            current.started,
            current.queued.track.id.clone(),
            current.queued.batch_id.clone(),
        )
    };
    if already_started {
        return Ok(json!({ "ok": true, "duplicate": already_started, "feedback_queued": true }));
    }
    if mode != "wave" {
        if let Some(current) = queue.current.as_mut() {
            current.started = true;
        }
        return Ok(json!({ "ok": true, "feedback_queued": true }));
    }
    let session_id = queue.wave_session_id.clone().ok_or_else(|| anyhow!("У активной волны отсутствует session ID"))?;
    enqueue_feedback(&session_id, "track_started", Some(&track_id), batch_id.as_deref(), None)?;
    if let Some(current) = queue.current.as_mut().filter(|current| current.ticket == ticket) {
        current.started = true;
    }
    Ok(json!({ "ok": true, "feedback_queued": true }))
}

async fn queue_advance(api: &YandexApi, ticket: u64, outcome: &str, seconds: f64) -> Result<Value> {
    if !valid_queue_outcome(outcome) {
        bail!("Неизвестный результат воспроизведения");
    }
    if !seconds.is_finite() || !(0.0..=86_400.0).contains(&seconds) {
        bail!("Некорректное время воспроизведения");
    }

    let mut queue = queue_engine().lock().await;
    if !queue.current.as_ref().map(|current| current.ticket == ticket).unwrap_or(false) {
        bail!("Результат относится к устаревшему ticket");
    }
    let current = queue.current.take().ok_or_else(|| anyhow!("Активный трек отсутствует"))?;

    if queue.mode == "wave" && current.started {
        if let Some(session_id) = queue.wave_session_id.clone() {
            let feedback = if outcome == "finished" { "track_finished" } else { "skip" };
            enqueue_feedback(
                &session_id,
                feedback,
                Some(&current.queued.track.id),
                current.queued.batch_id.as_deref(),
                Some(seconds),
            )?;
        }
    }

    if let Some(target) = outcome.strip_prefix("track:") {
        queue.release_payload();
        return Ok(json!({ "ok": true, "action": "mode", "mode": format!("track:{target}") }));
    }
    if let Some(target) = outcome.strip_prefix("playlist:") {
        queue.release_payload();
        return Ok(json!({ "ok": true, "action": "mode", "mode": format!("playlist:{target}") }));
    }

    let queue_mode = queue.mode.clone();
    match queue_mode.as_str() {
        "liked" => match outcome {
            "previous" => queue.liked_index = queue.liked_index.saturating_sub(1),
            "wave" | "liked" | "stock_radio" | "stop" | "idle" => {
                let next_mode = match outcome {
                    "wave" => "wave",
                    "liked" => "liked",
                    "stock_radio" => "external",
                    _ => "idle",
                };
                queue.release_payload();
                return Ok(json!({ "ok": true, "action": "mode", "mode": next_mode }));
            }
            _ => queue.liked_index = queue.liked_index.saturating_add(1),
        },
        "playlist" => match outcome {
            "previous" => queue.playlist_index = queue.playlist_index.saturating_sub(1),
            "wave" | "liked" | "stock_radio" | "stop" | "idle" => {
                let next_mode = match outcome {
                    "wave" => "wave",
                    "liked" => "liked",
                    "stock_radio" => "external",
                    _ => "idle",
                };
                queue.release_payload();
                return Ok(json!({ "ok": true, "action": "mode", "mode": next_mode }));
            }
            _ => queue.playlist_index = queue.playlist_index.saturating_add(1),
        },
        "track" => match outcome {
            "previous" => queue.search_index = queue.search_index.saturating_sub(1),
            "wave" | "liked" | "stock_radio" | "stop" | "idle" => {
                let next_mode = match outcome {
                    "wave" => "wave",
                    "liked" => "liked",
                    "stock_radio" => "external",
                    _ => "idle",
                };
                queue.release_payload();
                return Ok(json!({ "ok": true, "action": "mode", "mode": next_mode }));
            }
            _ => queue.search_index = queue.search_index.saturating_add(1),
        },
        "wave" => match outcome {
            "finished" | "dislike" => remember_wave_track(&mut queue, current.queued.clone()),
            "previous" => restore_previous_wave_track(&mut queue, current.queued.clone()),
            "wave" | "liked" | "stock_radio" | "stop" | "idle" => {
                let next_mode = match outcome {
                    "wave" => "wave",
                    "liked" => "liked",
                    "stock_radio" => "external",
                    _ => "idle",
                };
                queue.release_payload();
                return Ok(json!({ "ok": true, "action": "mode", "mode": next_mode }));
            }
            _ => {}
        },
        _ => {
            queue.release_payload();
            return Ok(json!({ "ok": true, "action": "mode", "mode": "idle" }));
        }
    }

    if outcome == "error" {
        queue.consecutive_errors = queue.consecutive_errors.saturating_add(1);
        if queue.consecutive_errors >= MAX_QUEUE_ERRORS {
            tracing::error!("[music-queue] Достигнут лимит последовательных ошибок воспроизведения");
            queue.release_payload();
            return Ok(json!({ "ok": true, "action": "mode", "mode": "idle", "reason": "too_many_errors" }));
        }
    } else {
        queue.consecutive_errors = 0;
    }
    queue_prepare_next(api, &mut queue).await
}

async fn queue_prepare_next(api: &YandexApi, queue: &mut QueueEngine) -> Result<Value> {
    if !matches!(queue.mode.as_str(), "liked" | "wave" | "track" | "playlist") {
        queue.release_payload();
        return Ok(json!({ "ok": true, "action": "mode", "mode": "idle" }));
    }
    for _ in 0..MAX_QUEUE_ERRORS {
        let queued = match queue.mode.as_str() {
            "liked" => {
                if queue.liked_index >= queue.liked_tracks.len() {
                    queue.liked_tracks = api.liked_tracks().await?;
                    queue.liked_index = 0;
                }
                let track = queue.liked_tracks.get(queue.liked_index)
                    .cloned().ok_or_else(|| anyhow!("Избранное не содержит доступных треков"))?;
                QueuedTrack { track, batch_id: None }
            }
            "playlist" => {
                if queue.playlist_tracks.is_empty() || queue.playlist_index >= queue.playlist_tracks.len() {
                    tracing::info!("[music-queue] Воспроизведение плейлиста завершено");
                    queue.release_payload();
                    return Ok(json!({ "ok": true, "action": "mode", "mode": "idle" }));
                }
                let track = queue.playlist_tracks.get(queue.playlist_index)
                    .cloned().ok_or_else(|| anyhow!("Плейлист не содержит доступных треков"))?;
                QueuedTrack { track, batch_id: None }
            }
            "track" => {
                if queue.search_tracks.is_empty() || queue.search_index >= queue.search_tracks.len() {
                    tracing::info!("[music-queue] Очередь поиска завершена");
                    queue.release_payload();
                    return Ok(json!({ "ok": true, "action": "mode", "mode": "idle" }));
                }
                let track = queue.search_tracks.get(queue.search_index)
                    .cloned().ok_or_else(|| anyhow!("Поисковая подборка пуста"))?;
                QueuedTrack { track, batch_id: None }
            }
            "wave" => {
                if queue.wave_pending.is_empty() {
                    flush_feedback().await?;
                    let session_id = queue.wave_session_id.clone()
                        .ok_or_else(|| anyhow!("У Моей волны нет активной Rotor session"))?;
                    let ids = queue.wave_played_ids.iter().cloned().collect::<Vec<_>>();
                    let more = api.wave_next(&session_id, &ids).await?;
                    let batch_id = more.get("batch_id").and_then(Value::as_str).map(str::to_owned);
                    let tracks = tracks_from_value(more.get("tracks"))?;
                    if tracks.is_empty() {
                        bail!("Моя волна не вернула следующую подборку");
                    }
                    queue.wave_pending.extend(tracks.into_iter().map(|track| QueuedTrack {
                        track,
                        batch_id: batch_id.clone(),
                    }));
                }
                queue.wave_pending.pop_front().ok_or_else(|| anyhow!("Очередь Моей волны неожиданно пуста"))?
            }
            _ => return Ok(json!({ "ok": true, "action": "mode", "mode": "idle" })),
        };

        if queue.mode == "wave" {
            let id = &queued.track.queue_id;
            if !queue.wave_played_ids.iter().any(|seen| seen == id) {
                queue.wave_played_ids.push_back(id.clone());
            }
            while queue.wave_played_ids.len() > MAX_WAVE_QUEUE_IDS {
                queue.wave_played_ids.pop_front();
            }
        }

        let audio = match api.resolve_audio(&queued.track.id).await {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!("[music-queue] Не удалось подготовить трек {}: {}", bounded(&queued.track.title, 80), safe_error(&error.to_string()));
                if queue.mode == "liked" {
                    queue.liked_index = queue.liked_index.saturating_add(1);
                } else if queue.mode == "playlist" {
                    queue.playlist_index = queue.playlist_index.saturating_add(1);
                } else if queue.mode == "track" {
                    queue.search_index = queue.search_index.saturating_add(1);
                }
                queue.consecutive_errors = queue.consecutive_errors.saturating_add(1);
                if queue.consecutive_errors >= MAX_QUEUE_ERRORS {
                    queue.release_payload();
                    return Ok(json!({ "ok": true, "action": "mode", "mode": "idle", "reason": "too_many_prepare_errors" }));
                }
                continue;
            }
        };
        let url = audio.get("url").and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Яндекс не вернул MP3 URL"))?;
        let bitrate = audio.get("bitrate_kbps").and_then(Value::as_u64).unwrap_or(0);
        queue.next_ticket = queue.next_ticket.wrapping_add(1).max(1);
        let ticket = queue.next_ticket;
        queue.current = Some(ActiveTrack {
            ticket,
            queued: queued.clone(),
            started: false,
        });
        return Ok(json!({
            "ok": true,
            "action": "play",
            "ticket": ticket,
            "mode": queue.mode,
            "track": queued.track,
            "audio_url": url,
            "bitrate_kbps": bitrate
        }));
    }
    queue.release_payload();
    Ok(json!({ "ok": true, "action": "mode", "mode": "idle", "reason": "prepare_error_limit" }))
}

fn tracks_from_value(value: Option<&Value>) -> Result<Vec<MusicTrack>> {
    let Some(value) = value else { bail!("Яндекс не вернул список треков") };
    let tracks: Vec<MusicTrack> = serde_json::from_value(value.clone())
        .context("Яндекс вернул некорректные метаданные треков")?;
    if tracks.len() > MAX_TRACKS_PER_QUEUE {
        bail!("Партия треков превышает допустимый размер");
    }
    Ok(tracks)
}

pub fn validate_credentials_file() -> Result<()> {
    read_token().map(|_| ())
}

fn read_token() -> Result<String> {
    for path in CREDENTIAL_PATHS {
        let mut options = OpenOptions::new();
        options.read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let file = match options.open(path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let metadata = file.metadata().context("Не удалось проверить credentials.json")?;
        if !metadata.is_file() || metadata.len() > MAX_CREDENTIAL_BYTES {
            bail!("credentials.json должен быть обычным файлом размером не более 16 KiB");
        }
        if metadata.permissions().mode() & 0o177 != 0 {
            tracing::warn!("credentials.json имеет открытые права (желательно chmod 600)");
        }
        let mut raw = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_CREDENTIAL_BYTES + 1).read_to_end(&mut raw)
            .context("Не удалось прочитать credentials.json")?;
        if raw.len() as u64 > MAX_CREDENTIAL_BYTES {
            bail!("credentials.json слишком большой");
        }
        let value: Value = serde_json::from_slice(&raw).context("credentials.json содержит некорректный JSON")?;
        let token = value.get("access_token").and_then(Value::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty() && token.len() <= 8192 && !token.chars().any(char::is_control))
            .ok_or_else(|| anyhow!("В credentials.json отсутствует корректный access_token"))?;
        return Ok(token.to_owned());
    }
    bail!("Не удалось найти credentials.json (проверены: /data/capsula-hub, /data/capsula-yandex, /opt/etc/capsula-yandex)");
}

async fn read_limited_body(mut response: Response, max_bytes: usize) -> Result<Vec<u8>> {
    // Do not reserve the full 2 MiB limit for every concurrent request.
    // Grow only as chunks arrive; the hard response limit remains unchanged.
    let mut body = Vec::with_capacity(initial_body_capacity(response.content_length(), max_bytes));
    while let Some(chunk) = response.chunk().await.context("Ошибка чтения HTTP body")? {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            bail!("HTTP body превышает допустимый размер {} байт", max_bytes);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn validate_feedback_response(status: StatusCode, body: &[u8]) -> Result<()> {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        bail!("Яндекс отклонил авторизацию Rotor feedback (HTTP {})", status.as_u16());
    }
    if !status.is_success() {
        bail!("Яндекс вернул HTTP {} для Rotor feedback", status.as_u16());
    }
    // The feedback endpoint can acknowledge an event with an empty 2xx body.
    if body.is_empty() || body.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(());
    }
    let value: Value = serde_json::from_slice(body)
        .context("Яндекс вернул некорректный JSON для Rotor feedback")?;
    if let Some(error) = value.get("error") {
        let name = error.get("name").and_then(Value::as_str).unwrap_or("API_ERROR");
        bail!("Яндекс отклонил Rotor feedback: {}", bounded(name, 80));
    }
    match value.get("result").and_then(Value::as_bool) {
        Some(true) => Ok(()),
        Some(false) => bail!("Яндекс вернул result=false для Rotor feedback"),
        None => bail!("Ответ Rotor feedback не содержит подтверждения result=true"),
    }
}

async fn read_api_response(response: Response) -> Result<Value> {
    let status = response.status();
    if response.content_length().map(|len| len > MAX_API_RESPONSE_BYTES as u64).unwrap_or(false) {
        bail!("Ответ Яндекс API превышает допустимый размер");
    }
    let bytes = read_limited_body(response, MAX_API_RESPONSE_BYTES)
        .await.context("Ошибка чтения ответа Яндекс API")?;
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        bail!("Яндекс API отклонил авторизацию (HTTP {})", status.as_u16());
    }
    if !status.is_success() {
        bail!("Яндекс API вернул HTTP {}", status.as_u16());
    }
    let value: Value = serde_json::from_slice(&bytes).context("Яндекс API вернул ответ не в формате JSON")?;
    if let Some(error) = value.get("error") {
        let name = error.get("name").and_then(Value::as_str).unwrap_or("API_ERROR");
        bail!("Яндекс API сообщил ошибку: {}", bounded(name, 80));
    }
    Ok(value)
}

fn result_object(value: &Value) -> &Value {
    value.get("result").filter(|result| result.is_object()).unwrap_or(value)
}

fn result_array(value: &Value) -> Option<&Vec<Value>> {
    value.get("result").and_then(Value::as_array).or_else(|| value.as_array())
}

fn string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| value.get(*field).and_then(|item| {
        item.as_str().map(str::to_owned)
            .or_else(|| item.as_i64().map(|number| number.to_string()))
            .or_else(|| item.as_u64().map(|number| number.to_string()))
    }))
}

fn parse_track_array(value: &Value) -> Vec<&Value> {
    if let Some(items) = result_array(value) {
        return items.iter().collect();
    }
    let result = result_object(value);
    result.get("tracks").and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}

fn parse_liked_refs(value: &Value) -> Vec<LikedTrackRef> {
    let result = result_object(value);
    let tracks = result.get("library")
        .and_then(|library| library.get("tracks"))
        .and_then(Value::as_array)
        .or_else(|| result.get("tracks").and_then(Value::as_array));
    tracks.map(|items| items.iter().filter_map(|item| {
        let id = string_field(item, &["id", "trackId", "track_id"])?;
        if validate_track_id(&id, "ID трека").is_err() { return None; }
        let album_id = string_field(item, &["albumId", "album_id"])
            .filter(|album| validate_track_id(album, "ID альбома").is_ok());
        Some(LikedTrackRef { id, album_id })
    }).collect()).unwrap_or_default()
}

fn parse_rotor_batch(value: &Value) -> RotorBatch {
    let batch_id = string_field(value, &["batchId", "batch_id"]);
    let tracks = value.get("sequence").and_then(Value::as_array)
        .map(|sequence| sequence.iter().filter_map(|item| {
            let track = item.get("track").filter(|track| track.is_object()).unwrap_or(item);
            parse_music_track(track, None)
        }).take(200).collect())
        .unwrap_or_default();
    RotorBatch { batch_id, tracks }
}

fn parse_music_track(track: &Value, preferred_album_id: Option<&str>) -> Option<MusicTrack> {
    let id = string_field(track, &["id", "realId", "real_id", "trackId", "track_id"])?;
    if validate_track_id(&id, "ID трека").is_err() { return None; }
    let album_id = preferred_album_id.map(str::to_owned).or_else(|| {
        track.get("albums").and_then(Value::as_array)
            .and_then(|albums| albums.first())
            .and_then(|album| string_field(album, &["id"]))
            .filter(|album| validate_track_id(album, "ID альбома").is_ok())
    });
    let queue_id = album_id.as_ref().map(|album| format!("{id}:{album}")).unwrap_or_else(|| id.clone());
    let title = track.get("title").and_then(Value::as_str)
        .map(|value| bounded(value, 160)).filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Без названия".into());
    let artist = track.get("artists").and_then(Value::as_array)
        .map(|artists| artists.iter().take(8)
            .filter_map(|artist| artist.get("name").and_then(Value::as_str))
            .map(|name| bounded(name, 80))
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>().join(", "))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Неизвестный исполнитель".into());
    let duration_ms = track.get("durationMs").or_else(|| track.get("duration_ms"))
        .and_then(Value::as_u64).filter(|duration| *duration <= 86_400_000);
    let cover_uri = track.get("coverUri").or_else(|| track.get("cover_uri"))
        .or_else(|| track.get("ogImage"))
        .and_then(Value::as_str)
        .or_else(|| {
            track.get("albums").and_then(Value::as_array)
                .and_then(|albums| albums.first())
                .and_then(|album| album.get("coverUri").or_else(|| album.get("cover_uri")))
                .and_then(Value::as_str)
        })
        .map(|v| bounded(v, 256));
    let normalization_gain = track.get("normalization")
        .and_then(|norm| norm.get("gain"))
        .and_then(Value::as_f64)
        .map(|g| g as f32)
        .or_else(|| {
            track.get("r128")
                .and_then(|r| r.get("i"))
                .and_then(Value::as_f64)
                .map(|i| (-14.0 - i) as f32)
        })
        .map(|g| g.clamp(-18.0, 8.0));
    Some(MusicTrack { id, queue_id, title, artist, duration_ms, album_id, cover_uri, normalization_gain })
}

fn validate_track_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 64 || !value.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':')) {
        bail!("Некорректный {label}");
    }
    Ok(())
}

fn validate_numeric_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 32 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("Некорректный {label}");
    }
    Ok(())
}

fn validate_queue_id(value: &str) -> Result<()> {
    let parts: Vec<&str> = value.split(':').collect();
    if !(1..=2).contains(&parts.len()) {
        bail!("Некорректный составной ID очереди");
    }
    validate_track_id(parts[0], "ID трека в очереди")?;
    if parts.len() == 2 {
        validate_track_id(parts[1], "ID альбома в очереди")?;
    }
    Ok(())
}

fn validate_session_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 180
        || !value.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~' | ':'))
        || value == "." || value == ".."
    {
        bail!("Яндекс вернул ID сессии в неожиданном формате");
    }
    Ok(())
}

fn validate_yandex_url(value: &str) -> Result<()> {
    let url = Url::parse(value).context("Яндекс вернул некорректную ссылку на аудиопоток")?;
    if url.scheme() != "https" || url.username() != "" || url.password().is_some()
        || url.fragment().is_some()
    {
        bail!("Яндекс вернул небезопасную ссылку на аудиопоток");
    }
    let host = url.host_str().ok_or_else(|| anyhow!("В ссылке Яндекса отсутствует host"))?;
    validate_media_host(host)
}

fn validate_media_host(host: &str) -> Result<()> {
    let lower = host.to_ascii_lowercase();
    let allowed = lower == "yandex.net" || lower.ends_with(".yandex.net")
        || lower == "yandex.ru" || lower.ends_with(".yandex.ru");
    if !allowed || lower.len() > 253 || lower.contains("..")
        || !lower.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-')
    {
        bail!("Яндекс вернул недопустимый media host");
    }
    Ok(())
}

fn xml_tag(xml: &str, tag: &str) -> Option<String> {
    let opening = format!("<{tag}>");
    let closing = format!("</{tag}>");
    let start = xml.find(&opening)? + opening.len();
    let end = xml[start..].find(&closing)? + start;
    let raw = xml[start..end].trim();
    if raw.is_empty() || raw.len() > 4096 { return None; }
    Some(raw.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", "\"").replace("&apos;", "'"))
}

fn build_feedback_payload(
    event: &str,
    track_id: Option<&str>,
    batch_id: Option<&str>,
    seconds: Option<f64>,
) -> Result<Value> {
    let event_name = match event {
        "radio_started" => "radioStarted",
        "track_started" => "trackStarted",
        "track_finished" => "trackFinished",
        "skip" => "skip",
        _ => bail!("Неизвестный тип Rotor feedback"),
    };
    let mut event_json = json!({
        "type": event_name,
        "timestamp": iso8601_timestamp()?
    });
    match event {
        "radio_started" => {}
        "track_started" => {
            let id = track_id.ok_or_else(|| anyhow!("Для trackStarted нужен ID трека"))?;
            validate_track_id(id, "ID трека Rotor")?;
            event_json["trackId"] = json!(id);
        }
        "track_finished" | "skip" => {
            let id = track_id.ok_or_else(|| anyhow!("Для события нужен ID трека"))?;
            validate_track_id(id, "ID трека Rotor")?;
            let played = seconds.ok_or_else(|| anyhow!("Для события нужна длительность прослушивания"))?;
            if !played.is_finite() || !(0.0..=86_400.0).contains(&played) {
                bail!("Некорректная длительность прослушивания");
            }
            event_json["trackId"] = json!(id);
            event_json["totalPlayedSeconds"] = json!(played);
        }
        _ => unreachable!(),
    }
    let mut payload = json!({ "event": event_json });
    if let Some(batch) = batch_id.filter(|value| !value.is_empty()) {
        if batch.len() > 180 || batch.chars().any(char::is_control) {
            bail!("Некорректный Rotor batch ID");
        }
        payload["batchId"] = json!(batch);
    }
    Ok(payload)
}

fn iso8601_timestamp() -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("Системное время раньше Unix epoch")?;
    let epoch_seconds = now.as_secs() as libc::time_t;
    let mut broken_down: libc::tm = unsafe { std::mem::zeroed() };
    let converted = unsafe { libc::gmtime_r(&epoch_seconds, &mut broken_down) };
    if converted.is_null() {
        bail!("Не удалось преобразовать время Rotor в UTC");
    }
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        broken_down.tm_year + 1900,
        broken_down.tm_mon + 1,
        broken_down.tm_mday,
        broken_down.tm_hour,
        broken_down.tm_min,
        broken_down.tm_sec,
        now.subsec_millis(),
    ))
}

fn bounded(value: &str, max_chars: usize) -> String {
    value.chars().filter(|ch| !ch.is_control()).take(max_chars).collect()
}

fn safe_error(value: &str) -> String {
    // Never pass through a signed URL, OAuth string or request path from an
    // upstream error. Keep only a short diagnostic class/message.
    let mut safe = value.to_owned();
    for marker in ["https://", "http://"] {
        while let Some(start) = safe.find(marker) {
            let end = safe[start..].find(char::is_whitespace).map(|n| start + n).unwrap_or(safe.len());
            safe.replace_range(start..end, "[URL REDACTED]");
        }
    }
    bounded(&safe, 240)
}

// Small self-contained MD5 implementation avoids adding a crypto dependency
// just for Yandex's legacy MP3 URL signature.
fn md5_hex(input: &[u8]) -> String {
    const S: [u32; 64] = [
        7,12,17,22,7,12,17,22,7,12,17,22,7,12,17,22,
        5,9,14,20,5,9,14,20,5,9,14,20,5,9,14,20,
        4,11,16,23,4,11,16,23,4,11,16,23,4,11,16,23,
        6,10,15,21,6,10,15,21,6,10,15,21,6,10,15,21,
    ];
    const K: [u32; 64] = [
        0xd76aa478,0xe8c7b756,0x242070db,0xc1bdceee,0xf57c0faf,0x4787c62a,0xa8304613,0xfd469501,
        0x698098d8,0x8b44f7af,0xffff5bb1,0x895cd7be,0x6b901122,0xfd987193,0xa679438e,0x49b40821,
        0xf61e2562,0xc040b340,0x265e5a51,0xe9b6c7aa,0xd62f105d,0x02441453,0xd8a1e681,0xe7d3fbc8,
        0x21e1cde6,0xc33707d6,0xf4d50d87,0x455a14ed,0xa9e3e905,0xfcefa3f8,0x676f02d9,0x8d2a4c8a,
        0xfffa3942,0x8771f681,0x6d9d6122,0xfde5380c,0xa4beea44,0x4bdecfa9,0xf6bb4b60,0xbebfbc70,
        0x289b7ec6,0xeaa127fa,0xd4ef3085,0x04881d05,0xd9d4d039,0xe6db99e5,0x1fa27cf8,0xc4ac5665,
        0xf4292244,0x432aff97,0xab9423a7,0xfc93a039,0x655b59c3,0x8f0ccc92,0xffeff47d,0x85845dd1,
        0x6fa87e4f,0xfe2ce6e0,0xa3014314,0x4e0811a1,0xf7537e82,0xbd3af235,0x2ad7d2bb,0xeb86d391,
    ];
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut message = input.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 { message.push(0); }
    message.extend_from_slice(&bit_len.to_le_bytes());

    let (mut a0, mut b0, mut c0, mut d0) = (0x67452301u32, 0xefcdab89u32, 0x98badcfeu32, 0x10325476u32);
    for chunk in message.chunks_exact(64) {
        let mut words = [0u32; 16];
        for (index, bytes) in chunk.chunks_exact(4).enumerate() {
            words[index] = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for index in 0..64 {
            let (f, g) = match index {
                0..=15 => ((b & c) | ((!b) & d), index),
                16..=31 => ((d & b) | ((!d) & c), (5 * index + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * index + 5) % 16),
                _ => (c ^ (b | !d), (7 * index) % 16),
            };
            let next = a.wrapping_add(f).wrapping_add(K[index]).wrapping_add(words[g]);
            let rotated = next.rotate_left(S[index]);
            let new_b = b.wrapping_add(rotated);
            a = d; d = c; c = b; b = new_b;
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut digest = Vec::with_capacity(16);
    digest.extend_from_slice(&a0.to_le_bytes());
    digest.extend_from_slice(&b0.to_le_bytes());
    digest.extend_from_slice(&c0.to_le_bytes());
    digest.extend_from_slice(&d0.to_le_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_matches_standard_test_vectors() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(md5_hex(b"message digest"), "f96b697d7cb7938d525a2f31aaf161d0");
    }

    #[test]
    fn parses_rotor_sequence_into_playable_tracks() {
        let value = json!({
            "batchId": "batch-1",
            "sequence": [
                {"track": {"id": 12, "title": "Песня", "durationMs": 181000,
                    "artists": [{"name": "Артист"}], "albums": [{"id": 34}] }},
                {"track": {"id": "56", "title": "Вторая", "artists": []}}
            ]
        });
        let batch = parse_rotor_batch(&value);
        assert_eq!(batch.batch_id.as_deref(), Some("batch-1"));
        assert_eq!(batch.tracks.len(), 2);
        assert_eq!(batch.tracks[0].queue_id, "12:34");
        assert_eq!(batch.tracks[0].artist, "Артист");
        assert_eq!(batch.tracks[1].queue_id, "56");
    }

    #[test]
    fn parses_liked_track_references() {
        let value = json!({ "result": { "library": { "tracks": [
            {"id": 12, "albumId": 34}, {"id": "56", "albumId": 78}
        ]}}});
        let refs = parse_liked_refs(&value);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].id, "12");
        assert_eq!(refs[0].album_id.as_deref(), Some("34"));
    }

    #[test]
    fn rejects_invalid_queue_and_session_ids() {
        assert!(validate_queue_id("123:456").is_ok());
        assert!(validate_queue_id("../other").is_err());
        assert!(validate_queue_id("123:456:789").is_err());
        assert!(validate_queue_id("123::456").is_err());
        assert!(validate_queue_id(":456").is_err());
        assert!(validate_session_id("session_123").is_ok());
        assert!(validate_session_id("../../settings").is_err());
    }

    #[test]
    fn validates_only_yandex_https_media_urls() {
        assert!(validate_yandex_url("https://storage.mds.yandex.net/audio" ).is_ok());
        assert!(validate_yandex_url("http://storage.mds.yandex.net/audio").is_err());
        assert!(validate_yandex_url("https://example.org/audio").is_err());
        assert!(validate_yandex_url("https://user:pass@storage.mds.yandex.net/audio").is_err());
    }

    #[test]
    fn radio_started_feedback_uses_session_batch_without_seed_as_from() {
        let payload = build_feedback_payload("radio_started", None, Some("batch-1"), None).unwrap();
        assert_eq!(payload["event"]["type"], "radioStarted");
        assert!(payload.get("from").is_none());
        assert_eq!(payload["batchId"], "batch-1");
    }

    #[test]
    fn builds_rotor_feedback_with_required_fields_and_batch_id() {
        let started = build_feedback_payload("track_started", Some("123"), Some("batch-7"), None).unwrap();
        assert_eq!(started["event"]["type"], "trackStarted");
        assert_eq!(started["event"]["trackId"], "123");
        assert_eq!(started["batchId"], "batch-7");
        let timestamp = started["event"]["timestamp"].as_str().unwrap();
        assert!(timestamp.ends_with('Z'));
        assert_eq!(timestamp.len(), 24);
        assert_eq!(&timestamp[4..5], "-");
        assert_eq!(&timestamp[10..11], "T");

        let finished = build_feedback_payload("track_finished", Some("123"), None, Some(42.5)).unwrap();
        assert_eq!(finished["event"]["type"], "trackFinished");
        assert_eq!(finished["event"]["totalPlayedSeconds"], 42.5);
        assert!(finished.get("batchId").is_none());
        assert!(build_feedback_payload("skip", Some("123"), None, None).is_err());
        assert!(build_feedback_payload("track_started", Some("bad-id"), None, None).is_err());
        assert!(build_feedback_payload("track_started", Some("123:456"), None, None).is_err());
        assert!(build_feedback_payload("skip", Some("123:456"), None, Some(0.0)).is_err());
    }

    #[test]
    fn feedback_queue_drops_overflow_without_growing() {
        let (sender, mut receiver) = mpsc::channel(1);
        let make_job = || FeedbackJob::Event {
            session_id: "session".into(),
            event: "track_started".into(),
            track_id: Some("123".into()),
            batch_id: None,
            seconds: None,
        };
        assert!(try_send_feedback(&sender, make_job(), "track_started").is_ok());
        assert!(try_send_feedback(&sender, make_job(), "track_started").is_ok());
        assert!(matches!(receiver.try_recv(), Ok(FeedbackJob::Event { .. })));
        assert!(receiver.try_recv().is_err(), "overflow event must be dropped, not queued");
        assert_eq!(sender.max_capacity(), 1);
    }

    #[test]
    fn queue_engine_reset_clears_previous_mode_and_history() {
        let mut queue = QueueEngine::default();
        queue.mode = "wave".into();
        queue.liked_index = 42;
        queue.wave_session_id = Some("session".into());
        queue.wave_played_ids.push_back("123:456".into());
        queue.consecutive_errors = 3;
        queue.next_ticket = 55;
        queue.reset("liked");
        assert_eq!(queue.mode, "liked");
        assert_eq!(queue.liked_index, 0);
        assert!(queue.wave_session_id.is_none());
        assert!(queue.wave_played_ids.is_empty());
        assert_eq!(queue.consecutive_errors, 0);
        assert_eq!(queue.next_ticket, 55, "ticket counter must not reset, otherwise an old player response could match a new track");
        assert!(queue.current.is_none());
    }

    #[test]
    fn idle_release_drops_large_queue_allocations_and_preserves_ticket() {
        let mut queue = QueueEngine::default();
        queue.liked_tracks = Vec::with_capacity(MAX_TRACKS_PER_QUEUE);
        queue.wave_pending = VecDeque::with_capacity(MAX_WAVE_QUEUE_IDS);
        queue.wave_history = VecDeque::with_capacity(MAX_WAVE_HISTORY);
        queue.wave_played_ids = VecDeque::with_capacity(MAX_WAVE_QUEUE_IDS);
        queue.next_ticket = 77;
        queue.release_payload();
        assert_eq!(queue.mode, "idle");
        assert_eq!(queue.next_ticket, 77);
        assert_eq!(queue.liked_tracks.capacity(), 0);
        assert_eq!(queue.wave_pending.capacity(), 0);
        assert_eq!(queue.wave_history.capacity(), 0);
        assert_eq!(queue.wave_played_ids.capacity(), 0);
    }

    #[test]
    fn response_body_initial_reservation_is_small_and_bounded() {
        assert_eq!(initial_body_capacity(Some(2 * 1024 * 1024), MAX_API_RESPONSE_BYTES), 64 * 1024);
        assert_eq!(initial_body_capacity(Some(4096), MAX_API_RESPONSE_BYTES), 4096);
        assert_eq!(initial_body_capacity(Some(9000), 2048), 2048);
        assert_eq!(initial_body_capacity(None, MAX_API_RESPONSE_BYTES), 0);
    }

    #[test]
    fn queue_track_payload_requires_bounded_metadata() {
        let tracks = json!([{
            "id": "123", "queue_id": "123:456", "title": "Песня",
            "artist": "Артист", "duration_ms": 180000, "album_id": "456"
        }]);
        let parsed = tracks_from_value(Some(&tracks)).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].queue_id, "123:456");
        let invalid = json!({"not": "an array"});
        assert!(tracks_from_value(Some(&invalid)).is_err());
    }

    #[test]
    fn queue_outcome_validation_is_explicit() {
        let accepted = ["finished", "next", "previous", "wave", "liked", "stock_radio", "stop", "idle", "error", "dislike"];
        for value in accepted {
            assert!(matches!(value, "finished" | "next" | "previous" | "wave" | "liked" | "stock_radio" | "stop" | "idle" | "error" | "dislike"));
        }
        assert!(accepted.iter().all(|value| valid_queue_outcome(value)));
        assert!(valid_queue_outcome("track:последний день лета"));
        assert!(valid_queue_outcome("playlist:новинки"));
        assert!(!valid_queue_outcome("launch_shell"));
        assert!(!valid_queue_outcome(""));
    }

    #[test]
    fn wave_history_is_bounded_and_previous_pops_latest_finished_track() {
        let mut queue = QueueEngine::default();
        for id in 0..(MAX_WAVE_HISTORY + 5) {
            remember_wave_track(&mut queue, QueuedTrack {
                track: MusicTrack {
                    id: id.to_string(), queue_id: id.to_string(), title: format!("Track {id}"),
                    artist: "Artist".into(), duration_ms: None, album_id: None, cover_uri: None,
                    normalization_gain: None,
                },
                batch_id: None,
            });
        }
        assert_eq!(queue.wave_history.len(), MAX_WAVE_HISTORY);
        assert_eq!(queue.wave_history.back().unwrap().track.id, "104");

        let current = QueuedTrack {
            track: MusicTrack {
                id: "current".into(), queue_id: "current".into(), title: "Current".into(),
                artist: "Artist".into(), duration_ms: None, album_id: None, cover_uri: None,
                normalization_gain: None,
            },
            batch_id: None,
        };
        restore_previous_wave_track(&mut queue, current);
        assert_eq!(queue.wave_pending.len(), 2);
        assert_eq!(queue.wave_pending[0].track.id, "104");
        assert_eq!(queue.wave_pending[1].track.id, "current");
    }

    #[test]
    fn reads_xml_download_fields() {
        let xml = "<download-info><host>storage.mds.yandex.net</host><path>/track/a&amp;b</path><ts>123</ts><s>sig</s></download-info>";
        assert_eq!(xml_tag(xml, "host").as_deref(), Some("storage.mds.yandex.net"));
        assert_eq!(xml_tag(xml, "path").as_deref(), Some("/track/a&b"));
        assert_eq!(xml_tag(xml, "ts").as_deref(), Some("123"));
        assert_eq!(xml_tag(xml, "s").as_deref(), Some("sig"));
    }
}
