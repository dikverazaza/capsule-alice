//! Интеграция с локальным LAPI voice-agent на порту 80.
//!
//! Автоматически находит процесс voice-agent, в рантайме открывает сервисные
//! методы (/create_text_phrase, /play, /light_sensor) и предоставляет API
//! для отправки голосовых фраз Марусе и прямого воспроизведения аудио.

use anyhow::{bail, Context, Result};
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    time::Duration,
};
use tokio::time::sleep;

const LAPI_PORT: u16 = 80;
const PATCH_OFFSET: u64 = 0x13ce10;
// AArch64 NOP instruction: 0xd503201f (little-endian: 1f 20 03 d5)
const NOP_BYTES: [u8; 4] = [0x1f, 0x20, 0x03, 0xd5];
// Исходная инструкция: cbz w0, #0x13d2c4 (0x340025a0 -> a0 25 00 34)
const CBZ_BYTES: [u8; 4] = [0xa0, 0x25, 0x00, 0x34];

/// Находит PID процесса voice-agent (-mode=app).
pub fn find_voice_agent_app_pid() -> Option<u32> {
    let entries = fs::read_dir("/proc").ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_str()?;
        let pid: u32 = name_str.parse().ok()?;

        let Ok(cmdline) = fs::read_to_string(format!("/proc/{pid}/cmdline")) else { continue };
        if cmdline.contains("voice-agent") && cmdline.contains("-mode=app") {
            return Some(pid);
        }
    }
    None
}

/// Находит базовый адрес загрузки voice-agent в памяти процесса.
pub fn find_voice_agent_base_address(pid: u32) -> Option<u64> {
    let maps = fs::read_to_string(format!("/proc/{pid}/maps")).ok()?;
    for line in maps.lines() {
        if line.contains("/usr/bin/voice-agent") && line.contains("r-xp") {
            let addr_str = line.split('-').next()?;
            return u64::from_str_radix(addr_str, 16).ok();
        }
    }
    None
}

/// Проверяет и при необходимости применяет NOP-патч в памяти процесса voice-agent.
pub fn ensure_voice_agent_patched() -> Result<bool> {
    let Some(pid) = find_voice_agent_app_pid() else {
        bail!("Процесс voice-agent -mode=app не найден");
    };

    let Some(base_addr) = find_voice_agent_base_address(pid) else {
        bail!("Не удалось определить базовый адрес voice-agent в PID {pid}");
    };

    let target_addr = base_addr + PATCH_OFFSET;
    let mem_path = format!("/proc/{pid}/mem");

    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&mem_path)
        .with_context(|| format!("Не удалось открыть {mem_path}"))?;

    file.seek(SeekFrom::Start(target_addr))?;
    let mut current = [0u8; 4];
    file.read_exact(&mut current)?;

    if current == NOP_BYTES {
        // Уже пропатчен
        return Ok(true);
    }

    if current != CBZ_BYTES {
        tracing::warn!(
            "[voice-lapi] Неожиданные байты по адресу 0x{:x} в PID {}: {:x?}",
            target_addr,
            pid,
            current
        );
    }

    file.seek(SeekFrom::Start(target_addr))?;
    file.write_all(&NOP_BYTES)?;
    file.flush()?;

    // Проверяем результат записи
    file.seek(SeekFrom::Start(target_addr))?;
    let mut verify = [0u8; 4];
    file.read_exact(&mut verify)?;

    if verify == NOP_BYTES {
        tracing::info!(
            "[voice-lapi] Успешно активирован полный LAPI (патч по адресу 0x{:x} в PID {})",
            target_addr,
            pid
        );
        Ok(true)
    } else {
        bail!("Не удалось верифицировать патч LAPI (прочитано: {:x?})", verify);
    }
}

/// Запускает фоновый наблюдатель, гарантирующий, что при перезапуске voice-agent
/// LAPI снова будет мгновенно открыт.
pub fn spawn_lapi_supervisor() {
    tokio::spawn(async {
        loop {
            if let Err(e) = ensure_voice_agent_patched() {
                tracing::debug!("[voice-lapi] Проверка LAPI: {e}");
            }
            sleep(Duration::from_secs(30)).await;
        }
    });
}

fn url_encode(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len() * 3);
    for b in input.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(encoded, "%{:02X}", b);
            }
        }
    }
    encoded
}

/// Отправляет текстовую фразу напрямую в движок Маруси (voice-agent) через LAPI.
/// Колонка обработает её как живой голосовой запрос и ответит своим родным голосом!
pub async fn send_text_phrase(http: &reqwest::Client, text: &str) -> Result<()> {
    let _ = ensure_voice_agent_patched();
    let encoded = url_encode(text);
    let url = format!("http://127.0.0.1:{LAPI_PORT}/create_text_phrase?text={encoded}");

    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .context("Ошибка запроса /create_text_phrase")?;

    if !resp.status().is_success() {
        bail!("voice-agent вернул статус {}", resp.status());
    }

    Ok(())
}

/// Воспроизводит аудио (URL или локальный файл) через нативный плеер voice-agent.
pub async fn play_url(http: &reqwest::Client, url_or_path: &str) -> Result<()> {
    let _ = ensure_voice_agent_patched();
    let target = if url_or_path.starts_with("http://")
        || url_or_path.starts_with("https://")
        || url_or_path.starts_with("file:")
    {
        url_or_path.to_owned()
    } else {
        format!("file:{url_or_path}")
    };

    let encoded = url_encode(&target);
    let url = format!("http://127.0.0.1:{LAPI_PORT}/play?url={encoded}");

    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .context("Ошибка запроса /play")?;

    if !resp.status().is_success() {
        bail!("voice-agent /play вернул статус {}", resp.status());
    }

    Ok(())
}

/// Считывает текущее значение датчика освещенности (люксы).
/// Прямое чтение через sysfs IIO или через LAPI voice-agent.
pub fn read_ambient_lux() -> Result<f32> {
    const IIO_PATH: &str =
        "/sys/devices/platform/soc/ffd00000.cbus/ffd1e000.i2c/i2c-0/0-0029/iio:device0/in_illuminance_both_input";

    if let Ok(raw) = fs::read_to_string(IIO_PATH) {
        if let Ok(val) = raw.trim().parse::<f32>() {
            return Ok(val);
        }
    }

    bail!("Датчик освещенности недоступен")
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct NativePhrase {
    pub id: &'static str,
    pub title: &'static str,
    pub path: &'static str,
    pub emotion: Option<&'static str>,
}

/// Список официальных встроенных реплик и звуков Маруси.
pub fn get_native_phrases() -> &'static [NativePhrase] {
    &[
        NativePhrase {
            id: "experiment",
            title: "🧪 Я люблю эксперименты",
            path: "/usr/share/voice-agent/Experiment.opus",
            emotion: Some("happy"),
        },
        NativePhrase {
            id: "neural_net",
            title: "🧠 Нейросеть",
            path: "/usr/share/voice-agent/Neural_net.opus",
            emotion: Some("interest"),
        },
        NativePhrase {
            id: "save_head",
            title: "🛡️ Береги голову",
            path: "/usr/share/voice-agent/Save_head.opus",
            emotion: Some("cunning"),
        },
        NativePhrase {
            id: "sotryasenie",
            title: "⚡ Сотрясение!",
            path: "/usr/share/voice-agent/Sotryasenie.opus",
            emotion: Some("fear"),
        },
        NativePhrase {
            id: "ukochalo",
            title: "🌀 Укачало...",
            path: "/usr/share/voice-agent/Ukochalo.opus",
            emotion: Some("surprised"),
        },
        NativePhrase {
            id: "where_am_i",
            title: "❓ Где я?",
            path: "/usr/share/voice-agent/Where_am_I.opus",
            emotion: Some("search"),
        },
        NativePhrase {
            id: "timer",
            title: "⏳ Звук таймера",
            path: "/usr/share/voice-agent/10_timer_1.opus",
            emotion: None,
        },
        NativePhrase {
            id: "alarm",
            title: "⏰ Звук будильника",
            path: "/usr/share/voice-agent/09_alarm.opus",
            emotion: None,
        },
        NativePhrase {
            id: "reminder",
            title: "🔔 Напоминание",
            path: "/usr/share/voice-agent/11_reminder.opus",
            emotion: None,
        },
    ]
}
