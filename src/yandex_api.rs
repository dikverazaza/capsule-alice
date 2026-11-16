//! Non-playback Yandex Music API probe used while migrating the player to Rust.
//!
//! This module only creates a temporary Rotor session and requests its next
//! batch. It deliberately does not send playback feedback or start audio. The
//! live playback runtime is the in-process Rust PlaybackSupervisor.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::{header, Client, Response, StatusCode};
use serde_json::{json, Value};
use std::{fs::OpenOptions, io::Read, sync::atomic::{AtomicU64, Ordering}, time::{Duration, SystemTime, UNIX_EPOCH}};

const API_BASE: &str = "https://api.music.yandex.net";
const CREDENTIALS_PATH: &str = "/opt/etc/capsula-yandex/credentials.json";
const MAX_CREDENTIAL_BYTES: u64 = 16 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_SAMPLE_TRACKS: usize = 5;
const USER_AGENT: &str = "CapsulaHub/0.2.9 (Linux; aarch64)";
const YANDEX_MUSIC_CLIENT: &str = "YandexMusicAndroid/24023621";
const PROBE_COOLDOWN_SECS: u64 = 30;
static LAST_PROBE_AT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct BatchPreview {
    count: usize,
    queue_ids: Vec<String>,
    samples: Vec<Value>,
}

/// Verify credentials, create an isolated `user:onyourwave` rotor session,
/// and ask the API for the next batch using the first batch as queue context.
/// No feedback events are sent, so no track is marked as played or skipped.
pub async fn probe_my_wave() -> Result<Value> {
    let token = read_token()?;
    acquire_probe_slot()?;
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .context("Не удалось создать HTTP-клиент Яндекс Музыки")?;

    let created = post_json(
        &client,
        &token,
        "rotor/session/new",
        json!({
            "seeds": ["user:onyourwave"],
            "includeTracksInResponse": true
        }),
    )
    .await
    .context("Не удалось создать тестовую сессию Моей волны")?;

    let session = result_object(&created);
    let session_id = string_field(session, &["radioSessionId", "radio_session_id"])
        .ok_or_else(|| anyhow!("Яндекс API не вернул radioSessionId"))?;
    validate_session_id(&session_id)?;
    let first_batch_id = string_field(session, &["batchId", "batch_id"]);
    let first = parse_batch(session);

    if first.count == 0 {
        bail!("Сессия создана, но первая подборка не содержит треков");
    }

    // Request a second batch without claiming any playback events. The queue
    // only tells Rotor which items were already returned in this diagnostic
    // session; it does not mark them as heard.
    let next_value = post_json(
        &client,
        &token,
        &format!("rotor/session/{session_id}/tracks"),
        json!({
            "queue": first.queue_ids.clone(),
            "feedbacks": []
        }),
    )
    .await;

    let next = match next_value {
        Ok(value) => {
            let batch = parse_batch(result_object(&value));
            json!({
                "ok": true,
                "track_count": batch.count,
                "samples": batch.samples,
                "batch_id_received": string_field(result_object(&value), &["batchId", "batch_id"]).is_some()
            })
        }
        Err(error) => json!({
            "ok": false,
            "error": safe_error(&error.to_string())
        }),
    };

    let next_batch_ok = next.get("ok").and_then(Value::as_bool).unwrap_or(false);

    Ok(json!({
        "ok": next_batch_ok,
        "provider": "Yandex Music",
        "account_authorized": true,
        "wave_seed": "user:onyourwave",
        "session_created": true,
        "session_id_returned": true,
        "first_batch": {
            "track_count": first.count,
            "batch_id_received": first_batch_id.is_some(),
            "samples": first.samples
        },
        "next_batch": next,
        "next_batch_ok": next_batch_ok,
        "playback_started": false,
        "feedback_sent": false,
        "note": "Это диагностическая сессия. Музыка не запускалась, обратная связь Rotor не отправлялась."
    }))
}

fn acquire_probe_slot() -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    loop {
        let previous = LAST_PROBE_AT.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(previous);
        if previous != 0 && elapsed < PROBE_COOLDOWN_SECS {
            bail!("Повторная проверка Яндекс API будет доступна через {} сек.", PROBE_COOLDOWN_SECS - elapsed);
        }
        if LAST_PROBE_AT.compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            return Ok(());
        }
    }
}

fn read_token() -> Result<String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }

    let file = options
        .open(CREDENTIALS_PATH)
        .with_context(|| format!("Не удалось безопасно открыть {CREDENTIALS_PATH}"))?;
    let metadata = file.metadata().context("Не удалось проверить credentials.json")?;
    if !metadata.is_file() || metadata.len() > MAX_CREDENTIAL_BYTES {
        bail!("credentials.json должен быть обычным файлом размером не более 16 KiB");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("Слишком открытые права credentials.json; установите chmod 600");
        }
    }

    let mut raw = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut raw)
        .context("Не удалось прочитать credentials.json")?;
    if raw.len() as u64 > MAX_CREDENTIAL_BYTES {
        bail!("credentials.json слишком большой");
    }

    let value: Value = serde_json::from_slice(&raw).context("credentials.json содержит некорректный JSON")?;
    let token = value
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 8192 && !value.chars().any(char::is_control))
        .ok_or_else(|| anyhow!("В credentials.json отсутствует корректный access_token"))?;
    Ok(token.to_owned())
}

async fn post_json(client: &Client, token: &str, path: &str, payload: Value) -> Result<Value> {
    let authorization = format!("OAuth {token}");
    let response = client
        .post(format!("{API_BASE}/{path}"))
        .header(header::AUTHORIZATION, authorization)
        .header("X-Yandex-Music-Client", YANDEX_MUSIC_CLIENT)
        .header(header::ACCEPT, "application/json")
        .json(&payload)
        .send()
        .await
        .context("Не удалось связаться с API Яндекс Музыки")?;

    read_api_response(response).await
}

async fn read_api_response(response: Response) -> Result<Value> {
    let status = response.status();
    if response
        .content_length()
        .map(|length| length > MAX_RESPONSE_BYTES as u64)
        .unwrap_or(false)
    {
        bail!("Ответ Яндекс API превышает допустимый размер");
    }

    let bytes = response.bytes().await.context("Ошибка чтения ответа Яндекс API")?;
    if bytes.len() > MAX_RESPONSE_BYTES {
        bail!("Ответ Яндекс API превышает допустимый размер");
    }

    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        bail!("Яндекс API отклонил авторизацию (HTTP {})", status.as_u16());
    }
    if !status.is_success() {
        bail!("Яндекс API вернул HTTP {}", status.as_u16());
    }

    let value: Value = serde_json::from_slice(&bytes)
        .context("Яндекс API вернул ответ не в формате JSON")?;
    if let Some(error) = value.get("error") {
        let name = error.get("name").and_then(Value::as_str).unwrap_or("API_ERROR");
        let message = error.get("message").and_then(Value::as_str).unwrap_or("ошибка API");
        bail!("Яндекс API: {} — {}", bounded(name, 80), bounded(message, 180));
    }
    Ok(value)
}

fn result_object(value: &Value) -> &Value {
    value.get("result").filter(|result| result.is_object()).unwrap_or(value)
}

fn string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| value.get(*field).and_then(|item| {
        item.as_str().map(str::to_owned)
            .or_else(|| item.as_i64().map(|number| number.to_string()))
            .or_else(|| item.as_u64().map(|number| number.to_string()))
    }))
}

fn validate_session_id(session_id: &str) -> Result<()> {
    if session_id.is_empty()
        || session_id.len() > 180
        || !session_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | '~' | ':'))
        || session_id == "."
        || session_id == ".."
    {
        bail!("Яндекс API вернул идентификатор сессии в неожиданном формате");
    }
    Ok(())
}

fn parse_batch(value: &Value) -> BatchPreview {
    let Some(sequence) = value.get("sequence").and_then(Value::as_array) else {
        return BatchPreview { count: 0, queue_ids: Vec::new(), samples: Vec::new() };
    };

    let mut queue_ids = Vec::new();
    let mut samples = Vec::new();
    let mut count = 0usize;

    for item in sequence {
        let track = item.get("track").filter(|track| track.is_object()).unwrap_or(item);
        let Some(track_id) = string_field(track, &["id", "trackId", "track_id"]) else {
            continue;
        };
        count += 1;

        let album_id = track
            .get("albums")
            .and_then(Value::as_array)
            .and_then(|albums| albums.first())
            .and_then(|album| string_field(album, &["id"]));
        let queue_id = album_id
            .map(|album| format!("{track_id}:{album}"))
            .unwrap_or_else(|| track_id.clone());
        queue_ids.push(queue_id);

        if samples.len() < MAX_SAMPLE_TRACKS {
            let title = track.get("title").and_then(Value::as_str).map(|text| bounded(text, 120))
                .unwrap_or_else(|| "Без названия".into());
            let artist = track.get("artists").and_then(Value::as_array)
                .map(|artists| {
                    artists.iter().take(4)
                        .filter_map(|artist| artist.get("name").and_then(Value::as_str))
                        .map(|name| bounded(name, 60))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .filter(|artist| !artist.is_empty())
                .unwrap_or_else(|| "Неизвестный исполнитель".into());
            samples.push(json!({ "title": title, "artist": artist }));
        }
    }

    BatchPreview { count, queue_ids, samples }
}

fn bounded(value: &str, max_chars: usize) -> String {
    value.chars().filter(|character| !character.is_control()).take(max_chars).collect()
}

fn safe_error(value: &str) -> String {
    bounded(value, 240)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwraps_api_result_envelope() {
        let value = json!({ "result": { "radioSessionId": "abc123", "sequence": [] } });
        assert_eq!(string_field(result_object(&value), &["radioSessionId"]).as_deref(), Some("abc123"));
    }

    #[test]
    fn parses_rotor_tracks_and_queue_ids() {
        let value = json!({
            "sequence": [
                { "track": { "id": 123, "title": "Тест", "artists": [{"name": "Группа"}], "albums": [{"id": 456}] } },
                { "track": { "id": "789", "title": "Второй", "artists": [{"name": "Артист"}] } }
            ]
        });
        let batch = parse_batch(&value);
        assert_eq!(batch.count, 2);
        assert_eq!(batch.queue_ids, vec!["123:456", "789"]);
        assert_eq!(batch.samples[0]["title"], "Тест");
        assert_eq!(batch.samples[0]["artist"], "Группа");
    }

    #[test]
    fn rejects_session_ids_that_could_change_the_request_path() {
        assert!(validate_session_id("abc123-xy").is_ok());
        assert!(validate_session_id("abc:123_xy").is_ok());
        assert!(validate_session_id(".").is_err());
        assert!(validate_session_id("..").is_err());
        assert!(validate_session_id("../other-endpoint").is_err());
        assert!(validate_session_id("id/extra").is_err());
    }

    #[test]
    fn ignores_control_characters_in_display_fields() {
        assert_eq!(bounded("A\nB\u{7}C", 10), "ABC");
    }
}
