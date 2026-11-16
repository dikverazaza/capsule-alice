//! Voice-agent event parser and command classifier.
//!
//! Raw /dev/log datagrams are parsed here in Rust. Compact typed events are
//! consumed directly by the Rust PlaybackSupervisor. PulseAudio stream
//! operations are delegated to the Rust PulseStreamTracker, without modifying
//! the system PulseAudio daemon.

use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};
use std::time::{Duration, Instant};

const FOLLOWUP_WINDOW: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VoiceRelayEvent {
    PhraseStart { followup: bool },
    NightLight { on: bool },
    Screen { mode: String },
    StockRadio,
    Command {
        command: String,
        phrase: String,
        skill: String,
        intent: String,
        vk_fallback: bool,
    },
    Fallback {
        phrase: String,
        skill: String,
        intent: String,
    },
}

#[derive(Default)]
pub struct VoiceEventParser {
    armed: bool,
    followup_deadline: Option<Instant>,
}

impl VoiceEventParser {
    /// Process one complete syslog datagram. The Capsule voice-agent normally
    /// emits one event per datagram; unrelated processes are ignored.
    pub fn process(&mut self, packet: &[u8]) -> Option<VoiceRelayEvent> {
        let line = String::from_utf8_lossy(packet).replace('\0', "");
        if !line.contains("voice-agent") {
            return None;
        }
        let lower = line.to_lowercase();

        // Native VK playlist startup is emitted independently of the ASR event.
        if lower.contains("media_player.cpp:") && lower.contains("music: set playlist") {
            self.followup_deadline = None;
            self.armed = false;
            return Some(VoiceRelayEvent::StockRadio);
        }

        if line.contains("OnPhraseStart") {
            if lower.contains("activation type: keyword") {
                self.armed = true;
                self.followup_deadline = None;
                return Some(VoiceRelayEvent::PhraseStart { followup: false });
            }
            if lower.contains("activation type: listen command") {
                let followup = self
                    .followup_deadline
                    .map(|deadline| Instant::now() <= deadline)
                    .unwrap_or(false);
                if followup {
                    self.armed = true;
                    self.followup_deadline = None;
                    return Some(VoiceRelayEvent::PhraseStart { followup: true });
                }
                self.armed = false;
            }
            return None;
        }

        if !line.contains("OnResponseComplete") || !self.armed {
            return None;
        }

        let (phrase, skill, intent) = response_fields(&line);
        let lower_phrase = phrase.to_lowercase();
        let night_source = if lower_phrase.is_empty() { lower.clone() } else { lower_phrase.clone() };

        if night_source.contains("экран") || night_source.contains("дисплей") || night_source.contains("часы") || night_source.contains("глаз") || night_source.contains("спать") || night_source.contains("спокойной ночи") || night_source.contains("засни") || night_source.contains("усни") {
            if night_source.contains("спать") || night_source.contains("спокойной ночи") || night_source.contains("засни") || night_source.contains("усни") || night_source.contains("сон") {
                self.followup_deadline = None;
                self.armed = false;
                return Some(VoiceRelayEvent::Screen { mode: "sleep".to_owned() });
            }
            if night_source.contains("глазк") || night_source.contains("глаза") {
                self.followup_deadline = None;
                self.armed = false;
                return Some(VoiceRelayEvent::Screen { mode: "eyes".to_owned() });
            }
            if night_source.contains("выключ") || night_source.contains("отключ") || night_source.contains("погаси") || night_source.contains("потуши") {
                self.followup_deadline = None;
                self.armed = false;
                return Some(VoiceRelayEvent::Screen { mode: "off".to_owned() });
            }
            if night_source.contains("включ") || night_source.contains("покажи") || night_source.contains("зажги") || night_source.contains("верни") || night_source.contains("время") {
                self.followup_deadline = None;
                self.armed = false;
                return Some(VoiceRelayEvent::Screen { mode: "clock".to_owned() });
            }
        }

        if night_source.contains("ночн") {
            let off = night_source.contains("выключ")
                || night_source.contains("отключ")
                || night_source.contains("погаси")
                || night_source.contains("выключить");
            self.followup_deadline = None;
            self.armed = false;
            return Some(VoiceRelayEvent::NightLight { on: !off });
        }
        let mut command = classify_command(&line, &phrase, &skill, &intent);
        let vk_fallback = is_vk_fallback(&lower);

        if command.is_none() && vk_fallback && (skill == "music" || intent.starts_with("music.")) && !phrase.is_empty() {
            let mut cleaned = phrase.as_str();
            for prefix in &["включи ", "поставь ", "запусти ", "сыграй "] {
                if let Some(rest) = cleaned.strip_prefix(prefix) {
                    cleaned = rest;
                    break;
                }
            }
            let trimmed = cleaned.trim();
            if !trimmed.is_empty() {
                command = Some(format!("track:{trimmed}"));
            }
        }

        if let Some(command) = command {
            if command == "stock_radio" {
                self.followup_deadline = None;
            } else if (matches!(command.as_str(), "wave" | "liked") || command.starts_with("track:") || command.starts_with("playlist:")) && vk_fallback {
                self.followup_deadline = Some(Instant::now() + FOLLOWUP_WINDOW);
            }
            self.armed = false;
            return Some(VoiceRelayEvent::Command {
                command,
                phrase,
                skill,
                intent,
                vk_fallback,
            });
        }

        if vk_fallback {
            if intent == "radio.play" {
                self.followup_deadline = Some(Instant::now() + FOLLOWUP_WINDOW);
            }
            self.armed = false;
            return Some(VoiceRelayEvent::Fallback { phrase, skill, intent });
        }

        if !phrase.is_empty() || !skill.is_empty() || !intent.is_empty() {
            self.followup_deadline = None;
            self.armed = false;
        }
        None
    }
}

fn response_fields(line: &str) -> (String, String, String) {
    let parsed = line
        .split_once("OnResponseComplete")
        .and_then(|(_, tail)| tail.find('{').map(|start| &tail[start..]))
        .and_then(|json| Deserializer::from_str(json).into_iter::<Value>().next())
        .and_then(Result::ok);

    let result = parsed
        .as_ref()
        .and_then(|value| value.get("result"))
        .filter(|value| value.is_object())
        .or(parsed.as_ref());

    let phrase = bounded_field(
        result
            .and_then(|value| value.get("asr_text"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| extract_json_string(line, "asr_text"))
            .unwrap_or_default(),
        300,
    );
    let skill = bounded_field(
        result
            .and_then(|value| value.get("skill"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| extract_json_string(line, "skill"))
            .unwrap_or_default(),
        120,
    );
    let intent = bounded_field(
        result
            .and_then(|value| value.get("intent"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| extract_json_string(line, "intent"))
            .unwrap_or_default(),
        120,
    );
    (phrase, skill, intent)
}


fn bounded_field(value: String, max_chars: usize) -> String {
    value
        .trim()
        .chars()
        .filter(|character| !character.is_control())
        .flat_map(char::to_lowercase)
        .map(|character| if character == 'ё' { 'е' } else { character })
        .take(max_chars)
        .collect()
}

/// Recover an escaped JSON string when the whole event object is truncated or
/// has extra non-JSON text around it.
fn extract_json_string(line: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\"");
    let marker_pos = line.find(&marker)?;
    let after_key = &line[marker_pos + marker.len()..];
    let colon_pos = after_key.find(':')?;
    let after_colon = after_key[colon_pos + 1..].trim_start();
    if !after_colon.starts_with('"') {
        return None;
    }

    let bytes = after_colon.as_bytes();
    let mut escaped = false;
    for index in 1..bytes.len() {
        match bytes[index] {
            b'\\' if !escaped => escaped = true,
            b'"' if !escaped => {
                return serde_json::from_str::<String>(&after_colon[..=index]).ok();
            }
            _ => escaped = false,
        }
    }
    None
}

fn extract_playlist_phrase(phrase: &str, intent: &str) -> Option<String> {
    let lower_phrase = phrase.to_lowercase();
    let words = words(&lower_phrase);
    let playlist_pos = words.iter().position(|w| w.starts_with("плейлист"));
    if let Some(pos) = playlist_pos {
        let after_words = &words[pos + 1..];
        let mut query = after_words.join(" ");
        for marker in &["под названием ", "с названием ", "по имени "] {
            if let Some(rest) = query.strip_prefix(marker) {
                query = rest.to_owned();
            }
        }
        let trimmed = query.trim();
        if trimmed == "мой" || trimmed == "мои" || trimmed == "свой" || trimmed == "свои" || trimmed.is_empty() {
            return Some(String::new());
        }
        return Some(trimmed.to_owned());
    }
    if intent == "music.play_playlist" {
        let mut query = lower_phrase;
        for verb in &["включи ", "поставь ", "запусти ", "сыграй "] {
            if let Some(rest) = query.strip_prefix(verb) {
                query = rest.to_owned();
            }
        }
        let trimmed = query.trim();
        if trimmed == "мой" || trimmed == "мои" || trimmed == "плейлист" || trimmed.is_empty() {
            return Some(String::new());
        }
        return Some(trimmed.to_owned());
    }
    None
}

fn extract_track_query(phrase: &str, words: &[String], skill: &str, intent: &str) -> Option<String> {
    let track_keywords = [
        "трек", "треки", "песню", "песня", "песни", "композицию", "композиция",
        "группу", "группа", "исполнителя", "исполнитель", "альбом"
    ];
    for (i, w) in words.iter().enumerate() {
        if track_keywords.iter().any(|k| w.starts_with(k)) {
            let after = words[i + 1..].join(" ");
            let trimmed = after.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    if let Some(first) = words.first() {
        if first.starts_with("найди") || first.starts_with("сыграй") || first.starts_with("поищи") {
            let after = words[1..].join(" ");
            let trimmed = after.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    let is_music_context = skill == "music"
        || intent.starts_with("music.play_")
        || intent == "music.play";

    if is_music_context {
        if let Some(first) = words.first() {
            if first.starts_with("включ") || first.starts_with("постав") || first.starts_with("запуст") {
                let after = words[1..].join(" ");
                let trimmed = after.trim();
                if !trimmed.is_empty() && !trimmed.starts_with("радио") {
                    return Some(trimmed.to_owned());
                }
            }
        }
        let whole = words.join(" ");
        let trimmed = whole.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("радио") {
            return Some(trimmed.to_owned());
        }
    }

    None
}

fn extract_sleep_timer(words: &[String], phrase: &str) -> Option<String> {
    let lower = phrase.to_lowercase();

    // Cancellation check: "отмени таймер сна", "выключи таймер", etc.
    if (lower.contains("таймер") || lower.contains("таймера"))
        && (lower.contains("отмен") || lower.contains("сброс") || lower.contains("выключ") || lower.contains("удал"))
    {
        return Some("sleep_timer:cancel".into());
    }

    // Setting check: must have "через" or "таймер сна" or "таймер музыки"
    let is_timer_candidate = lower.contains("через")
        || lower.contains("таймер сна")
        || lower.contains("таймер музыки")
        || (lower.contains("таймер") && (lower.contains("минут") || lower.contains("час")));

    if !is_timer_candidate {
        return None;
    }

    if lower.contains("полтора часа") || lower.contains("полтора") {
        return Some("sleep_timer:5400".into());
    }
    if lower.contains("полчаса") || lower.contains("пол часа") {
        return Some("sleep_timer:1800".into());
    }

    let mut seconds: u64 = 0;
    let mut current_num: Option<u64> = None;

    for word in words {
        let w = word.as_str();
        if let Ok(n) = w.parse::<u64>() {
            current_num = Some(n);
            continue;
        }

        let parsed_word_num = match w {
            "один" | "одна" | "одну" | "одно" => Some(1),
            "два" | "две" => Some(2),
            "три" => Some(3),
            "четыре" => Some(4),
            "пять" => Some(5),
            "шесть" => Some(6),
            "семь" => Some(7),
            "восемь" => Some(8),
            "девять" => Some(9),
            "десять" => Some(10),
            "одиннадцать" => Some(11),
            "двенадцать" => Some(12),
            "тринадцать" => Some(13),
            "четырнадцать" => Some(14),
            "пятнадцать" => Some(15),
            "двадцать" => Some(20),
            "тридцать" => Some(30),
            "сорок" => Some(40),
            "пятьдесят" => Some(50),
            "шестьдесят" => Some(60),
            _ => None,
        };

        if let Some(wn) = parsed_word_num {
            current_num = match current_num {
                Some(prev) if (20..=50).contains(&prev) && wn < 10 => Some(prev + wn),
                _ => Some(wn),
            };
            continue;
        }

        if w.starts_with("час") {
            let count = current_num.take().unwrap_or(1);
            seconds += count * 3600;
        } else if w.starts_with("минут") {
            let count = current_num.take().unwrap_or(1);
            seconds += count * 60;
        } else if w.starts_with("секунд") {
            let count = current_num.take().unwrap_or(1);
            seconds += count;
        }
    }

    if seconds > 0 {
        let clamped = seconds.clamp(10, 43200);
        Some(format!("sleep_timer:{clamped}"))
    } else {
        None
    }
}

fn classify_command(line: &str, phrase: &str, skill: &str, intent: &str) -> Option<String> {
    let words = words(phrase);

    if let Some(cmd) = extract_sleep_timer(&words, phrase) {
        return Some(cmd);
    }

    if has_sequence_prefix(&words, "останови", &["музык"])
        || has_sequence_prefix(&words, "выключи", &["музык"])
        || has_sequence_prefix(&words, "стоп", &["музык"])
        || has_sequence_prefix(&words, "остановить", &["воспроизведен"])
    {
        return Some("stop".into());
    }

    if is_what_is_playing_action(&words, phrase, intent) {
        return Some("what_is_playing".into());
    }
    if is_like_action(&words, phrase, intent) {
        return Some("like".into());
    }
    if is_dislike_action(&words, phrase, intent) {
        return Some("dislike".into());
    }

    // Exact skill intent is authoritative for Yandex favourites, even when VK
    // also attaches a subscription hint and a fallback TTS response.
    if intent == "music.play_liked_playlist" {
        return Some("liked".into());
    }

    if let Some(query) = extract_playlist_phrase(phrase, intent) {
        return Some(format!("playlist:{query}"));
    }

    if is_wave_phrase(&words) {
        return Some("wave".into());
    }
    if is_liked_phrase(&words) {
        return Some("liked".into());
    }

    let lower = line.to_lowercase();
    if lower.contains("\"media_type\":\"radio\"")
        || lower.contains("\"media_type\": \"radio\"")
    {
        return Some("stock_radio".into());
    }

    let semantic = format!("{skill} {intent}");
    if ["my_wave", "mywave", "onyourwave", "rotor", "personal_wave"]
        .iter()
        .any(|marker| semantic.contains(marker))
    {
        return Some("wave".into());
    }
    if [
        "liked", "likes", "favorites", "favourites", "favourite_tracks", "favorite_tracks",
    ]
    .iter()
    .any(|marker| semantic.contains(marker))
    {
        return Some("liked".into());
    }
    if intent == "radio.play" && !phrase.is_empty() {
        return Some("stock_radio".into());
    }

    if has_prefix(&words, &["следующ", "пропуст"])
        || words.iter().any(|word| word.as_str() == "дальше")
        || has_sequence(&words, &["переключи", "трек"])
    {
        return Some("next".into());
    }
    if has_prefix(&words, &["предыдущ"])
        || has_sequence_prefix(&words, "прошл", &["трек", "песн"])
        || has_sequence(&words, &["верни", "песню"])
        || has_sequence(&words, &["назад", "на", "трек"])
    {
        return Some("previous".into());
    }

    if intent.contains("controls.pause")
        || lower.contains("pause_control")
        || words.iter().any(|word| word.as_str() == "пауза")
        || has_sequence(&words, &["поставь", "на", "паузу"])
    {
        return Some("pause".into());
    }
    if intent.contains("controls.resume")
        || lower.contains("resume_control")
        || has_prefix(&words, &["продолжи", "продолжить", "возобнови"])
    {
        return Some("resume".into());
    }
    if intent.contains("controls.stop") || lower.contains("stop_control") {
        return Some("stop".into());
    }
    if ["controls.next", "controls.skip", "next_track", "skip_track"]
        .iter()
        .any(|marker| intent.contains(marker))
        || ["\"next_control\"", "\"skip_control\"", "\"next_track_control\""]
            .iter()
            .any(|marker| lower.contains(marker))
    {
        return Some("next".into());
    }
    if ["controls.previous", "previous_track", "prev_track"]
        .iter()
        .any(|marker| intent.contains(marker))
        || ["\"previous_control\"", "\"prev_control\"", "\"previous_track_control\""]
            .iter()
            .any(|marker| lower.contains(marker))
    {
        return Some("previous".into());
    }

    if let Some(query) = extract_track_query(phrase, &words, skill, intent) {
        return Some(format!("track:{query}"));
    }

    None
}

fn is_vk_fallback(lower_line: &str) -> bool {
    [
        "subscription_required_hint",
        "подписки на vk музыку",
        "у вас пока нет подписки",
        "я пока не знаю такого радио",
        "простите, я пока не знаю такого радио",
    ]
    .iter()
    .any(|marker| lower_line.contains(marker))
}

fn is_like_action(words: &[String], _phrase: &str, intent: &str) -> bool {
    if (intent.contains("like") || intent.contains("thumbs_up")) && !intent.contains("dislike") && !intent.contains("play_liked") {
        return true;
    }
    if words.is_empty() {
        return false;
    }
    if words.iter().any(|w| w == "лайк" || w == "лайкни" || w == "лайкнуть") {
        if !has_prefix(words, &["включ", "запуст", "сыграй"]) {
            return true;
        }
    }
    has_sequence(words, &["поставь", "лайк"])
        || has_sequence(words, &["поставить", "лайк"])
        || has_sequence(words, &["мне", "нравится"])
        || (words.len() == 1 && words[0] == "нравится")
        || has_sequence(words, &["добавь", "в", "любимые"])
        || has_sequence(words, &["добавь", "в", "избранное"])
        || has_sequence(words, &["сохрани", "трек"])
        || has_sequence(words, &["сохрани", "песню"])
}

fn is_dislike_action(words: &[String], _phrase: &str, intent: &str) -> bool {
    if intent.contains("dislike") || intent.contains("thumbs_down") {
        return true;
    }
    if words.is_empty() {
        return false;
    }
    if words.iter().any(|w| w.starts_with("дизлайк")) {
        return true;
    }
    has_sequence(words, &["поставь", "дизлайк"])
        || has_sequence(words, &["поставить", "дизлайк"])
        || has_sequence(words, &["не", "нравится"])
        || has_sequence(words, &["мне", "не", "нравится"])
        || has_sequence(words, &["больше", "не", "включай"])
        || has_sequence(words, &["удали", "из", "любимых"])
        || has_sequence(words, &["удали", "из", "избранного"])
}

fn is_what_is_playing_action(_words: &[String], phrase: &str, intent: &str) -> bool {
    if intent.contains("what_is_playing") || intent.contains("current_track") || intent.contains("track_info") {
        return true;
    }
    let p = phrase.to_lowercase();
    p.contains("что играет")
        || p.contains("что сейчас играет")
        || p.contains("что это играет")
        || p.contains("что за трек")
        || p.contains("что за песня")
        || p.contains("кто поет")
        || p.contains("кто это поет")
        || p.contains("какая песня играет")
        || p.contains("какой трек играет")
        || p.contains("какая песня")
        || p.contains("назови трек")
        || p.contains("назови песню")
        || p.contains("название трека")
        || p.contains("название песни")
        || p.contains("как называется эта песня")
        || p.contains("как называется этот трек")
        || p.contains("как называется песня")
        || p.contains("как называется трек")
}

fn is_wave_phrase(words: &[String]) -> bool {
    has_sequence_prefix(words, "моя", &["волн"])
        || has_sequence_prefix(words, "мою", &["волн"])
        || has_sequence_prefix(words, "персональн", &["подборк", "волн"])
        || has_sequence(words, &["радио", "яндекс"])
        || has_sequence(words, &["яндекс", "радио", "яндекс"])
}

fn is_liked_phrase(words: &[String]) -> bool {
    has_sequence_prefix(words, "любим", &["трек", "песн", "музык"])
        || has_sequence_prefix(words, "мои", &["любим", "лайк", "сохранен"])
        || has_sequence_prefix(words, "моя", &["музык", "коллекц"])
        || has_sequence_prefix(words, "включ", &["избран", "любим"])
        || has_sequence_prefix(words, "постав", &["избран", "любим"])
        || words.iter().any(|w| w == "избранное")
        || has_sequence(words, &["мои", "треки"])
        || has_sequence(words, &["мои", "песни"])
        || has_sequence(words, &["понравившиеся", "треки"])
        || has_sequence(words, &["понравившиеся", "песни"])
}

fn words(value: &str) -> Vec<String> {
    let mut normalized = String::new();
    for character in value.chars().flat_map(char::to_lowercase) {
        let character = if character == 'ё' { 'е' } else { character };
        if character.is_alphanumeric() {
            normalized.push(character);
        } else {
            normalized.push(' ');
        }
    }
    normalized
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

fn has_prefix(words: &[String], prefixes: &[&str]) -> bool {
    words
        .iter()
        .any(|word| prefixes.iter().any(|prefix| word.starts_with(*prefix)))
}

fn has_sequence(words: &[String], sequence: &[&str]) -> bool {
    !sequence.is_empty()
        && words.windows(sequence.len()).any(|window| {
            window
                .iter()
                .zip(sequence.iter())
                .all(|(word, expected)| word.as_str() == *expected)
        })
}

fn has_sequence_prefix(words: &[String], first: &str, second: &[&str]) -> bool {
    words.windows(2).any(|window| {
        window[0].starts_with(first)
            && second
                .iter()
                .any(|prefix| window[1].starts_with(*prefix))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn datagram(payload: &str) -> Vec<u8> {
        format!("voice-agent app: {payload}").into_bytes()
    }

    fn arm(parser: &mut VoiceEventParser) {
        assert_eq!(
            parser.process(&datagram("OnPhraseStart activation type: keyword")),
            Some(VoiceRelayEvent::PhraseStart { followup: false })
        );
    }

    fn response(phrase: &str, intent: &str, extra: &str) -> Vec<u8> {
        datagram(&format!(
            "OnResponseComplete {{\"result\":{{\"asr_text\":{},\"skill\":\"music\",\"intent\":\"{}\"}}}} {}",
            serde_json::to_string(phrase).unwrap(), intent, extra
        ))
    }

    #[test]
    fn keyword_then_my_wave_is_classified() {
        let mut parser = VoiceEventParser::default();
        arm(&mut parser);
        let event = parser.process(&response("Маруся, включи мою волну", "radio.play", ""));
        assert!(matches!(event, Some(VoiceRelayEvent::Command { command, .. }) if command == "wave"));
    }

    #[test]
    fn liked_intent_is_authoritative_even_with_vk_fallback() {
        let mut parser = VoiceEventParser::default();
        arm(&mut parser);
        let event = parser.process(&response(
            "включи музыку",
            "music.play_liked_playlist",
            "subscription_required_hint",
        ));
        assert!(matches!(event, Some(VoiceRelayEvent::Command { command, vk_fallback: true, .. }) if command == "liked"));
    }

    #[test]
    fn ordinary_radio_is_not_misclassified_as_my_wave() {
        let mut parser = VoiceEventParser::default();
        arm(&mut parser);
        let event = parser.process(&response("включи радио", "radio.play", ""));
        assert!(matches!(event, Some(VoiceRelayEvent::Command { command, .. }) if command == "stock_radio"));
    }

    #[test]
    fn parser_ignores_responses_without_keyword_or_followup_activation() {
        let mut parser = VoiceEventParser::default();
        assert_eq!(parser.process(&response("включи мою волну", "radio.play", "")), None);
    }

    #[test]
    fn followup_listen_command_only_arms_inside_window() {
        let mut parser = VoiceEventParser::default();
        arm(&mut parser);
        let fallback = parser.process(&response("", "radio.play", "subscription_required_hint"));
        assert!(matches!(fallback, Some(VoiceRelayEvent::Fallback { .. })));
        assert_eq!(
            parser.process(&datagram("OnPhraseStart activation type: listen command")),
            Some(VoiceRelayEvent::PhraseStart { followup: true })
        );
    }

    #[test]
    fn native_media_player_announcement_switches_to_stock_radio() {
        let mut parser = VoiceEventParser::default();
        let event = parser.process(&datagram("media_player.cpp: MUSIC: set playlist"));
        assert_eq!(event, Some(VoiceRelayEvent::StockRadio));
    }

    #[test]
    fn night_light_on_and_off_are_typed_events() {
        let mut parser = VoiceEventParser::default();

        arm(&mut parser);
        assert_eq!(
            parser.process(&response("включи ночник", "", "")),
            Some(VoiceRelayEvent::NightLight { on: true })
        );

        arm(&mut parser);
        assert_eq!(
            parser.process(&response("выключи ночник", "", "")),
            Some(VoiceRelayEvent::NightLight { on: false })
        );
    }

    #[test]
    fn truncated_json_fields_are_recovered() {
        let line = r#"voice-agent OnResponseComplete broken {"result":{"asr_text":"Включи волну\u0021", "skill":"music", "intent":"radio.play""#;
        let (phrase, skill, intent) = response_fields(line);
        assert_eq!(phrase, "включи волну!");
        assert_eq!(skill, "music");
        assert_eq!(intent, "radio.play");
    }

    #[test]
    fn stock_voice_agent_testline_is_parsed_without_false_music_command() {
        let line = r#"<30>Nov  1 19:12:00 voice-agent: [phrase_result_getter.cpp:192] OnResponseComplete {"qid":"t","result":{"commands":[],"skill":"t","asr_text":"отмени выключение"}}"#;
        let (phrase, skill, intent) = response_fields(line);
        assert_eq!(phrase, "отмени выключение");
        assert_eq!(skill, "t");
        assert_eq!(intent, "");
        let mut parser = VoiceEventParser::default();
        assert_eq!(parser.process(line.as_bytes()), None);
    }

    #[test]
    fn explicit_pause_intent_works_without_asr_phrase() {
        assert_eq!(
            classify_command("voice-agent OnResponseComplete {\"pause_control\":true}", "", "", "controls.pause"),
            Some("pause".into())
        );
    }

    #[test]
    fn russian_command_variants_are_classified() {
        assert_eq!(classify_command("", "мои любимые песенки", "", ""), Some("liked".into()));
        assert_eq!(classify_command("", "переключи следующий трек", "", ""), Some("next".into()));
        assert_eq!(classify_command("", "верни песню", "", ""), Some("previous".into()));
        assert_eq!(classify_command("", "поставь на паузу", "", ""), Some("pause".into()));
        assert_eq!(classify_command("", "возобнови музыку", "", ""), Some("resume".into()));
        assert_eq!(classify_command("", "останови музыку", "", ""), Some("stop".into()));
        assert_eq!(classify_command("", "включи последний день лета", "music", "music.play_new_artist"), Some("track:последний день лета".into()));
        assert_eq!(classify_command("", "поставь песню группа крови", "", ""), Some("track:группа крови".into()));
        assert_eq!(classify_command("", "найди кукушка", "", ""), Some("track:кукушка".into()));
        assert_eq!(classify_command("", "включи мой плейлист", "", ""), Some("playlist:".into()));
        assert_eq!(classify_command("", "включи плейлист русский рок", "", ""), Some("playlist:русский рок".into()));
        assert_eq!(classify_command("", "поставь лайк", "", ""), Some("like".into()));
        assert_eq!(classify_command("", "лайк", "", ""), Some("like".into()));
        assert_eq!(classify_command("", "мне нравится", "", ""), Some("like".into()));
        assert_eq!(classify_command("", "поставь дизлайк", "", ""), Some("dislike".into()));
        assert_eq!(classify_command("", "дизлайк", "", ""), Some("dislike".into()));
        assert_eq!(classify_command("", "не нравится", "", ""), Some("dislike".into()));
        assert_eq!(classify_command("", "что сейчас играет", "", ""), Some("what_is_playing".into()));
        assert_eq!(classify_command("", "что играет", "", ""), Some("what_is_playing".into()));
        assert_eq!(classify_command("", "кто поет", "", ""), Some("what_is_playing".into()));
        assert_eq!(classify_command("", "выключи музыку через 15 минут", "", ""), Some("sleep_timer:900".into()));
        assert_eq!(classify_command("", "выключи через час", "", ""), Some("sleep_timer:3600".into()));
        assert_eq!(classify_command("", "таймер сна на полчаса", "", ""), Some("sleep_timer:1800".into()));
        assert_eq!(classify_command("", "отмени таймер сна", "", ""), Some("sleep_timer:cancel".into()));
        assert_eq!(classify_command("", "выключи таймер", "", ""), Some("sleep_timer:cancel".into()));
        assert_eq!(classify_command("", "выключи музыку", "", ""), Some("stop".into()));
    }

    #[test]
    fn track_search_with_vk_fallback_is_classified_as_command() {
        let mut parser = VoiceEventParser::default();
        arm(&mut parser);
        let event = parser.process(&response("включи последний день лета", "music.play_new_artist", "subscription_required_hint"));
        assert!(matches!(event, Some(VoiceRelayEvent::Command { command, vk_fallback: true, .. }) if command == "track:последний день лета"));
    }

    #[test]
    fn playlist_with_vk_fallback_is_classified_as_command() {
        let mut parser = VoiceEventParser::default();
        arm(&mut parser);
        let event = parser.process(&response("включи мой плейлист", "music.play_playlist", "subscription_required_hint"));
        assert!(matches!(event, Some(VoiceRelayEvent::Command { command, vk_fallback: true, .. }) if command == "playlist:"));
    }
}
