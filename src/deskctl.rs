use anyhow::{anyhow, Result};
use reqwest::Method;
use serde_json::Value;
use crate::config::AppConfig;

pub async fn request(
    client: &reqwest::Client,
    config: &AppConfig,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    if config.deskctl_url.is_empty() { return Err(anyhow!("Не указан адрес Deskctl в настройках")); }
    if config.deskctl_api_key.is_empty() { return Err(anyhow!("Не задан API-ключ Deskctl в настройках")); }
    let url = format!("{}{}", config.deskctl_url.trim_end_matches('/'), path);
    let mut request = client.request(method, url).bearer_auth(&config.deskctl_api_key);
    if let Some(json) = body { request = request.json(&json); }

    let response = request.send().await
        .map_err(|error| anyhow!("Не удалось подключиться к Deskctl на ПК: {error}"))?;
    let status = response.status();
    let raw = response.text().await
        .map_err(|error| anyhow!("Не удалось прочитать ответ Deskctl: {error}"))?;
    let data: Value = if raw.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(&raw)
            .map_err(|_| anyhow!("Deskctl вернул некорректный JSON (HTTP {})", status.as_u16()))?
    };

    if !status.is_success() {
        let detail = data.get("error").or_else(|| data.get("message"))
            .and_then(Value::as_str).unwrap_or("нет описания ошибки");
        return Err(anyhow!("Deskctl HTTP {}: {}", status.as_u16(), detail));
    }
    // Some Deskctl builds report application-level failures with HTTP 200.
    if data.get("ok").and_then(Value::as_bool) == Some(false)
        || data.get("success").and_then(Value::as_bool) == Some(false)
    {
        let detail = data.get("error").or_else(|| data.get("message"))
            .and_then(Value::as_str).unwrap_or("операция отклонена Deskctl");
        return Err(anyhow!("Deskctl: {}", detail));
    }
    Ok(data)
}
