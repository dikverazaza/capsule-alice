use crate::{state::AppState, wol};
use serde_json::Value;
use std::{io::SeekFrom, path::Path, time::{Duration, Instant}};
use tokio::{fs::File, io::{AsyncReadExt, AsyncSeekExt}, time::sleep};

pub fn spawn_monitor(state: AppState) {
    tokio::spawn(async move { monitor_loop(state).await; });
}

async fn monitor_loop(state: AppState) {
    let mut offset: u64 = 0;
    let mut inode: Option<u64> = None;
    let mut pending = Vec::<u8>::new();
    let mut last_trigger = Instant::now() - Duration::from_secs(20);
    let mut watched_path = String::new();
    let mut initialized = false;
    let mut skip_initial_contents = false;

    loop {
        let cfg = state.config.read().await.clone();
        if !cfg.voice_enabled {
            state.runtime.write().await.voice_monitor_state = "выключен в настройках".into();
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        if watched_path != cfg.voice_log_path {
            watched_path = cfg.voice_log_path.clone();
            offset = 0;
            inode = None;
            pending.clear();
            initialized = false;
            // Match the old tailer: skip historical data only when the file
            // already existed as monitoring began. A newly-created log starts
            // at byte zero so its first fresh event is not lost.
            skip_initial_contents = tokio::fs::metadata(&cfg.voice_log_path).await.is_ok();
        }
        let path = Path::new(&cfg.voice_log_path);
        let metadata = match tokio::fs::metadata(path).await {
            Ok(value) => value,
            Err(_) => {
                state.runtime.write().await.voice_monitor_state = format!("ожидание {}", cfg.voice_log_path);
                sleep(Duration::from_millis(800)).await;
                continue;
            }
        };
        let current_inode = file_identity(&metadata);
        if !initialized {
            // Do not replay old journal entries and unexpectedly wake the PC at boot.
            offset = if skip_initial_contents { metadata.len() } else { 0 };
            pending.clear();
            initialized = true;
        } else if metadata.len() < offset || inode.is_some() && current_inode.is_some() && inode != current_inode {
            offset = 0;
            pending.clear();
        }
        inode = current_inode;

        if metadata.len() > offset {
            match File::open(path).await {
                Ok(mut file) => {
                    if file.seek(SeekFrom::Start(offset)).await.is_ok() {
                        let mut chunk = Vec::new();
                        if file.read_to_end(&mut chunk).await.is_ok() {
                            offset += chunk.len() as u64;
                            pending.extend_from_slice(&chunk);
                            // Prevent a corrupt or unexpectedly huge partial line from growing forever.
                            if pending.len() > 96 * 1024 {
                                pending.clear();
                            }
                            while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                                let line: Vec<u8> = pending.drain(..=newline).collect();
                                let line = String::from_utf8_lossy(&line);
                                if let Some((phrase, _skill, intent)) = parse_voice_event(&line) {
                                    if phrase.is_empty() { continue; }
                                    // Игнорируем собственные синтетические команды синтеза Hub ("повтори фразу ...")
                                    if phrase.starts_with("повтори фразу") || intent == "voice_repeat.start" {
                                        continue;
                                    }
                                    state.runtime.write().await.last_voice = phrase.clone();

                                    // Голосовой запрос к нейросети ("Маруся, спроси у нейросети...")
                                    let is_dialog_pending = {
                                        let mut rt = state.runtime.write().await;
                                        if let Some(deadline) = rt.llm_dialog_pending_until {
                                            if tokio::time::Instant::now() < deadline {
                                                rt.llm_dialog_pending_until = None;
                                                true
                                            } else {
                                                rt.llm_dialog_pending_until = None;
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    };

                                    let query_opt = if is_dialog_pending {
                                        let lower = phrase.to_lowercase();
                                        if lower == "отмена" || lower == "стоп" || lower == "хватит" || lower == "забудь" || lower == "закрой" {
                                            let http_clone = state.http.clone();
                                            tokio::spawn(async move {
                                                let _ = crate::led_arbiter::trigger_emotion("happy", Some(Duration::from_secs(2)));
                                                let _ = crate::voice_agent_lapi::send_text_phrase(&http_clone, "повтори фразу Хорошо, отменяю").await;
                                            });
                                            continue;
                                        }
                                        // Пользователь произнес вопрос в режиме диалога: глушим штатный ответ VK и отправляем в LLM
                                        crate::pulse::suppress_vk_response("LLM диалог").await;
                                        Some(phrase.clone())
                                    } else if cfg.llm.enabled && cfg.llm.voice_trigger {
                                        match crate::llm::parse_llm_voice_intent(&phrase) {
                                            Some(crate::llm::LlmVoiceIntent::Ask(query)) => {
                                                // Прямой вопрос: глушим штатный отказ VK ("Я пока не могу...")
                                                crate::pulse::suppress_vk_response("LLM прямой запрос").await;
                                                Some(query)
                                            }
                                            Some(crate::llm::LlmVoiceIntent::StartDialog) => {
                                                // Активация режима нейросети без вопроса: глушим отказ VK и вежливо переспрашиваем
                                                crate::pulse::suppress_vk_response("LLM запуск диалога").await;
                                                state.runtime.write().await.llm_dialog_pending_until = Some(tokio::time::Instant::now() + Duration::from_secs(25));
                                                let http_clone = state.http.clone();
                                                tokio::spawn(async move {
                                                    let _ = crate::led_arbiter::trigger_emotion("interest", Some(Duration::from_secs(4)));
                                                    let _ = crate::voice_agent_lapi::send_text_phrase(&http_clone, "повтори фразу Слушаю вас! Что спросить у нейросети?").await;
                                                });
                                                continue;
                                            }
                                            None => None,
                                        }
                                    } else {
                                        None
                                    };

                                    if let Some(query) = query_opt {
                                        tracing::info!("[voice-llm] Отправка запроса к нейросети: '{}'", query);
                                        crate::pulse::play_ack_sound();
                                        let http_clone = state.http.clone();
                                        let llm_cfg = cfg.llm.clone();
                                        tokio::spawn(async move {
                                            let _ = crate::led_arbiter::trigger_emotion("search", Some(Duration::from_secs(12)));
                                            match crate::llm::ask_llm(&http_clone, &llm_cfg, &query, None).await {
                                                Ok(answer) => {
                                                    let _ = crate::led_arbiter::trigger_emotion("speaking", Some(Duration::from_secs(8)));
                                                    let repeat_phrase = format!("повтори фразу {}", answer);
                                                    let _ = crate::voice_agent_lapi::send_text_phrase(&http_clone, &repeat_phrase).await;
                                                }
                                                Err(err) => {
                                                    tracing::error!("[voice-llm] Ошибка LLM: {err}");
                                                    let _ = crate::led_arbiter::trigger_emotion("fear", Some(Duration::from_secs(3)));
                                                    let _ = crate::voice_agent_lapi::send_text_phrase(&http_clone, "повтори фразу Извините, не удалось получить ответ от нейросети").await;
                                                }
                                            }
                                        });
                                        continue;
                                    }

                                    if !is_power_management_phrase(&phrase)
                                        && matches_wake_phrase(&phrase, &cfg.voice_phrases)
                                        && last_trigger.elapsed() >= Duration::from_secs(12)
                                    {
                                        last_trigger = Instant::now();
                                        match wol::send_wake(&cfg) {
                                            Ok(()) => {
                                                let mut runtime = state.runtime.write().await;
                                                runtime.last_wake = phrase.clone();
                                                runtime.last_error.clear();
                                                runtime.wake_count += 1;
                                            }
                                            Err(error) => {
                                                state.runtime.write().await.last_error = error.to_string();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {}
            }
        }
        state.runtime.write().await.voice_monitor_state = "активен".into();
        sleep(Duration::from_millis(400)).await;
    }
}

#[cfg(unix)]
fn file_identity(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.ino())
}

#[cfg(not(unix))]
fn file_identity(_metadata: &std::fs::Metadata) -> Option<u64> { None }

fn parse_voice_event(line: &str) -> Option<(String, String, String)> {
    if !line.contains("voice-agent") || !line.contains("OnResponseComplete") { return None; }
    let marker = line.find("OnResponseComplete")?;
    let tail = &line[marker + "OnResponseComplete".len()..];
    let value = tail.find('{').and_then(|start| {
        let mut stream = serde_json::Deserializer::from_str(&tail[start..]).into_iter::<Value>();
        stream.next().and_then(Result::ok)
    });

    let result = value.as_ref().and_then(|v| v.get("result"));
    let phrase = result.and_then(|v| v.get("asr_text")).and_then(Value::as_str)
        .map(str::to_owned).or_else(|| extract_json_string(line, "asr_text")).unwrap_or_default();
    let skill = result.and_then(|v| v.get("skill")).and_then(Value::as_str)
        .map(str::to_owned).or_else(|| extract_json_string(line, "skill")).unwrap_or_default();
    let intent = result.and_then(|v| v.get("intent")).and_then(Value::as_str)
        .map(str::to_owned).or_else(|| extract_json_string(line, "intent")).unwrap_or_default();
    Some((phrase.trim().to_owned(), skill.to_lowercase(), intent.to_lowercase()))
}

// The stock log occasionally contains a JSON fragment that cannot be parsed
// as a complete object. Recover individual escaped string fields just as the
// previous Python listener did.
fn extract_json_string(line: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\"");
    let marker_pos = line.find(&marker)?;
    let after_key = &line[marker_pos + marker.len()..];
    let colon_pos = after_key.find(':')?;
    let after_colon = after_key[colon_pos + 1..].trim_start();
    if !after_colon.starts_with('"') { return None; }
    let bytes = after_colon.as_bytes();
    let mut escaped = false;
    for index in 1..bytes.len() {
        match bytes[index] {
            b'\\' if !escaped => escaped = true,
            b'"' if !escaped => return serde_json::from_str(&after_colon[..=index]).ok(),
            _ => escaped = false,
        }
    }
    None
}

fn normalize(value: &str) -> String {
    let mut out = String::new();
    let mut last_space = true;
    for ch in value.chars().flat_map(char::to_lowercase) {
        let ch = if ch == 'ё' { 'е' } else { ch };
        if ch.is_alphanumeric() {
            out.push(ch);
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_owned()
}


/// Power-management commands must never be treated as a request to wake the PC,
/// even if a broad configured wake phrase (for example, just "компьютер")
/// happens to match part of the utterance.
fn is_power_management_phrase(phrase: &str) -> bool {
    let value = normalize(phrase);
    [
        "выключи компьютер", "выключи пк", "выключить компьютер", "выключить пк",
        "выключение компьютера", "выключение пк",
        "перезагрузи компьютер", "перезагрузи пк",
        "перезагрузка компьютера", "перезагрузка пк",
        "переведи компьютер в сон", "отправь компьютер в сон",
        "переведи компьютер в гибернацию",
    ].iter().any(|needle| value.contains(needle))
}

fn matches_wake_phrase(phrase: &str, configured: &[String]) -> bool {
    let normalized = normalize(phrase);
    if normalized.is_empty() || ["не надо", "не нужно", "не включай", "не буди", "не запускай"]
        .iter().any(|negation| normalized.contains(negation))
    { return false; }
    let words: Vec<&str> = normalized.split_whitespace().collect();
    configured.iter().any(|candidate| {
        let key = normalize(candidate);
        let parts: Vec<&str> = key.split_whitespace().collect();
        !parts.is_empty() && words.windows(parts.len()).any(|window| window == parts.as_slice())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalizes_yo_and_punctuation() {
        assert_eq!(normalize("Включи, МОЙ компьютер!"), "включи мой компьютер");
        assert_eq!(normalize("Ёлка"), "елка");
    }
    #[test]
    fn matches_only_whole_phrase_and_respects_negation() {
        let phrases = vec!["включи компьютер".to_owned()];
        assert!(matches_wake_phrase("Маруся, включи компьютер", &phrases));
        assert!(!matches_wake_phrase("не включай компьютер", &phrases));
        assert!(!matches_wake_phrase("включи компьютерный стол", &phrases));
    }
    #[test]
    fn power_commands_are_never_wake_requests() {
        assert!(is_power_management_phrase("Выключи компьютер"));
        assert!(is_power_management_phrase("Перезагрузи ПК"));
        assert!(!is_power_management_phrase("Маруся, включи компьютер"));
    }
}
