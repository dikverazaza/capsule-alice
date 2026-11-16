//! Интеграция с Большими Языковыми Моделями (LLM).
//!
//! Позволяет задавать вопросы нейросети (DeepSeek, OpenAI, Groq, OpenRouter, Ollama)
//! и автоматически озвучивать сгенерированный ответ аутентичным голосом Маруси.

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LlmConfig {
    pub enabled: bool,
    pub provider: String,
    pub api_url: String,
    pub api_key: String,
    pub model: String,
    pub system_prompt: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub voice_trigger: bool,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            provider: "deepseek".into(),
            api_url: "https://api.deepseek.com/chat/completions".into(),
            api_key: String::new(),
            model: "deepseek-chat".into(),
            system_prompt: "Ты — Маруся. Отвечай кратко, емко и по существу (1-2 коротких предложения, до 25-30 слов) простым разговорным языком для мгновенной озвучки голосом. Не используй markdown-разметку, списки и спецсимволы.".into(),
            max_tokens: 90,
            temperature: 0.5,
            voice_trigger: true,
        }
    }
}

impl LlmConfig {
    pub fn validate(&mut self) -> Result<()> {
        self.provider = self.provider.trim().to_lowercase();
        if self.provider.is_empty() {
            self.provider = "deepseek".into();
        }

        self.api_url = self.api_url.trim().to_string();
        self.api_key = self.api_key.trim().to_string();
        self.model = self.model.trim().to_string();

        if self.max_tokens == 0 {
            self.max_tokens = 180;
        } else if self.max_tokens > 1000 {
            self.max_tokens = 1000;
        }

        if !(0.0..=2.0).contains(&self.temperature) {
            self.temperature = 0.7;
        }

        if self.system_prompt.trim().is_empty() {
            self.system_prompt = Self::default().system_prompt;
        }

        Ok(())
    }
}

/// Приводит URL к корректному endpoint `/chat/completions` в зависимости от провайдера.
pub fn resolve_endpoint(url: &str, provider: &str) -> String {
    let trimmed = url.trim();
    if !trimmed.is_empty() {
        if trimmed.ends_with("/chat/completions") {
            return trimmed.to_string();
        }
        return format!("{}/chat/completions", trimmed.trim_end_matches('/'));
    }

    match provider.to_lowercase().as_str() {
        "deepseek" => "https://api.deepseek.com/chat/completions".to_string(),
        "groq" => "https://api.groq.com/openai/v1/chat/completions".to_string(),
        "openrouter" => "https://openrouter.ai/api/v1/chat/completions".to_string(),
        "ollama" => "http://192.168.1.218:11434/v1/chat/completions".to_string(),
        _ => "https://api.openai.com/v1/chat/completions".to_string(),
    }
}

/// Подставляет название модели по умолчанию, если оно не задано пользователем.
pub fn resolve_model(model: &str, provider: &str) -> String {
    let trimmed = model.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }
    match provider.to_lowercase().as_str() {
        "deepseek" => "deepseek-chat".to_string(),
        "groq" => "llama-3.3-70b-versatile".to_string(),
        "openrouter" => "meta-llama/llama-3.3-70b-instruct:free".to_string(),
        "ollama" => "llama3".to_string(),
        _ => "gpt-4o-mini".to_string(),
    }
}

/// Очищает ответ нейросети от markdown и служебных символов для чистого звучания синтеза речи.
pub fn clean_for_speech(text: &str) -> String {
    let mut s = text.to_string();

    // Удаляем блоки кода ```...```
    while let Some(start) = s.find("```") {
        if let Some(end) = s[start + 3..].find("```") {
            s.replace_range(start..start + 3 + end + 3, "");
        } else {
            s.replace_range(start..start + 3, "");
        }
    }

    // Удаляем markdown ссылки [текст](url) -> оставляем только текст
    while let Some(open_sq) = s.find('[') {
        if let Some(close_sq) = s[open_sq + 1..].find(']') {
            let close_sq_idx = open_sq + 1 + close_sq;
            if s[close_sq_idx..].starts_with("](") {
                if let Some(close_paren) = s[close_sq_idx + 2..].find(')') {
                    let full_end = close_sq_idx + 2 + close_paren;
                    let label = s[open_sq + 1..close_sq_idx].to_string();
                    s.replace_range(open_sq..=full_end, &label);
                    continue;
                }
            }
        }
        break;
    }

    // Удаляем инлайн-код и markdown разметку
    s = s.replace('`', "")
        .replace('*', "")
        .replace('#', "")
        .replace('~', "")
        .replace('_', " ")
        .replace(['«', '»', '“', '”', '"'], " ");

    // Удаляем буллеты в начале строк
    let mut cleaned_lines = Vec::new();
    for line in s.lines() {
        let trimmed = line.trim();
        let stripped = trimmed.strip_prefix('-')
            .or_else(|| trimmed.strip_prefix('•'))
            .or_else(|| trimmed.strip_prefix('>'))
            .unwrap_or(trimmed)
            .trim();
        if !stripped.is_empty() {
            cleaned_lines.push(stripped);
        }
    }

    let joined = cleaned_lines.join(" ");
    let mut words = Vec::new();
    for w in joined.split_whitespace() {
        // Пропускаем ссылки
        if w.starts_with("http://") || w.starts_with("https://") {
            continue;
        }
        // Фильтруем эмодзи и спецсимволы
        let clean_word: String = w.chars().filter(|c| {
            let u = *c as u32;
            // Исключаем эмодзи и символы (диапазоны эмодзи Unicode)
            !(0x1F300..=0x1FAFF).contains(&u) &&
            !(0x2600..=0x27BF).contains(&u) &&
            !(0xFE00..=0xFE0F).contains(&u)
        }).collect();

        if !clean_word.is_empty() {
            words.push(clean_word);
        }
    }
    words.join(" ")
}

/// Отправляет запрос к LLM через стандартный OpenAI-совместимый протокол.
pub async fn ask_llm(
    http: &reqwest::Client,
    config: &LlmConfig,
    prompt: &str,
    system_override: Option<&str>,
) -> Result<String> {
    if !config.enabled {
        bail!("Модуль нейросети (LLM) отключен в настройках");
    }

    let endpoint = resolve_endpoint(&config.api_url, &config.provider);
    let model = resolve_model(&config.model, &config.provider);
    let system = system_override.unwrap_or(&config.system_prompt);

    let messages = serde_json::json!([
        { "role": "system", "content": system },
        { "role": "user", "content": prompt.trim() }
    ]);

    let mut body = serde_json::json!({
        "model": model,
        "messages": messages,
        "max_tokens": config.max_tokens,
        "temperature": config.temperature,
    });

    // Отключаем reasoning в DeepSeek/совместимых моделях для максимальной скорости (экономит 1-2 сек)
    if config.provider == "deepseek"
        || config.provider == "custom"
        || model.to_lowercase().contains("deepseek")
    {
        if let Some(map) = body.as_object_mut() {
            map.insert("thinking".to_string(), serde_json::json!({ "type": "disabled" }));
        }
    }

    let mut req = http.post(&endpoint)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(35))
        .json(&body);

    if !config.api_key.trim().is_empty() {
        req = req.bearer_auth(config.api_key.trim());
    }

    let resp = req.send().await
        .map_err(|e| anyhow!("Не удалось отправить запрос к LLM ({}): {e}", endpoint))?;

    let status = resp.status();
    let text = resp.text().await
        .map_err(|e| anyhow!("Ошибка чтения ответа LLM: {e}"))?;

    if !status.is_success() {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(msg) = val.get("error").and_then(|e| e.get("message")).and_then(|m| m.as_str()) {
                bail!("Ошибка LLM API ({}): {}", status, msg);
            }
        }
        bail!("LLM API вернул HTTP {}: {}", status, text);
    }

    let val: serde_json::Value = serde_json::from_str(&text)
        .context("Не удалось распарсить JSON ответа LLM")?;

    let content = val.get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c0| c0.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|cnt| cnt.as_str())
        .ok_or_else(|| anyhow!("В ответе LLM нет choices[0].message.content"))?;

    Ok(clean_for_speech(content))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmVoiceIntent {
    /// Пользователь сразу задал вопрос: "спроси у нейросети сколько планет..."
    Ask(String),
    /// Пользователь позвал нейросеть без вопроса: "спроси у нейросети", "нейросеть"
    StartDialog,
}

/// Распознает, является ли голосовая реплика запросом к нейросети или активацией диалога.
pub fn parse_llm_voice_intent(phrase: &str) -> Option<LlmVoiceIntent> {
    let lower = phrase.trim().to_lowercase();
    let prefixes = [
        "спроси у нейросети",
        "спроси у нейросеть",
        "спроси нейросеть",
        "вопрос к нейросети",
        "вопрос нейросети",
        "задай вопрос нейросети",
        "спроси у искусственного интеллекта",
        "спроси искусственный интеллект",
        "вопрос искусственному интеллекту",
        "спроси у ии",
        "спроси ии",
        "вопрос к ии",
        "вопрос ии",
        "спроси у gpt",
        "спроси у чат гпт",
        "спроси у чатгпт",
        "спроси у chatgpt",
        "спроси chatgpt",
        "спроси чатгпт",
        "спроси gpt",
        "спроси у чат бота",
        "спроси у чат-бота",
        "спроси чат бота",
        "включи нейросеть",
        "режим нейросети",
        "поговори с нейросетью",
        "нейросеть ответь",
        "нейросеть скажи",
        "нейросеть",
        "чат гпт",
        "чатгпт",
        "chatgpt",
    ];

    for prefix in &prefixes {
        if let Some(rest) = lower.strip_prefix(prefix) {
            let clean = rest
                .trim_start_matches(|c: char| c == ',' || c == ':' || c == '-' || c == '?' || c.is_whitespace())
                .trim();
            if clean.len() >= 3 {
                return Some(LlmVoiceIntent::Ask(clean.to_string()));
            } else {
                return Some(LlmVoiceIntent::StartDialog);
            }
        }
    }
    None
}

/// Распознает, является ли голосовая реплика прямым вопросом к нейросети.
pub fn extract_llm_query(phrase: &str) -> Option<String> {
    match parse_llm_voice_intent(phrase) {
        Some(LlmVoiceIntent::Ask(q)) => Some(q),
        _ => None,
    }
}
