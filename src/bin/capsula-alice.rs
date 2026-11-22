//! Autonomous Standalone Alice Assistant Daemon for VK Capsule Mini
//! Written in native Rust, compiled for aarch64-unknown-linux-musl.
//!
//! Fully independent of PC: Runs directly on the speaker hardware,
//! maintaining clock display, LED ring animations, touch sensors,
//! hardware volume buttons, Yandex Uniproxy conversational AI, and Yandex Music.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::Mutex,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncReadExt,
    sync::mpsc,
    time::sleep,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    WebSocketStream,
};

const UNIPROXY_URL: &str = "wss://uniproxy.alice.ya.ru/uni.ws";
const FIFO_RING: &str = "/tmp/ring.fifo";
/// Сколько Алиса ждёт НАЧАЛА фразы пользователя после wake-слова.
const COMMAND_LISTEN_WINDOW: Duration = Duration::from_secs(10);
/// Сколько Алиса ждёт продолжения диалога после окончания своей реплики.
const FOLLOWUP_WINDOW: Duration = Duration::from_secs(10);
/// Дополнительное окно, если ASR вернул пустоту (одна повторная попытка).
const COMMAND_RETRY_WINDOW: Duration = Duration::from_secs(3);
/// Файл-триггер локального wake word (в него пишет wakewordd).
const WAKE_TRIGGER_FILE: &str = "/tmp/wakeword.trigger";

// --- Local Features State (Timers, Alarms, Bluetooth, Whisper) ---
static WHISPER_MODE: AtomicBool = AtomicBool::new(false);
static BLUETOOTH_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
struct ActiveTimer {
    label: String,
    total_secs: u64,
    deadline: Instant,
    announce: Option<String>,
}

#[derive(Clone)]
struct ActiveAlarm {
    time_str: String,
    target_hour: u32,
    target_minute: u32,
    deadline: Instant,
}

// Feature 5: reminders are timers with a spoken label; DND suppresses them.
static DND_MODE: AtomicBool = AtomicBool::new(false);
// Feature 6: adaptive volume follows the measured noise floor. Disabled by default at night.
static ADAPTIVE_VOLUME: AtomicBool = AtomicBool::new(false);
static ADAPTIVE_BASE_VOLUME: AtomicU32 = AtomicU32::new(35);
static LAST_ADAPTIVE_TICK: Mutex<Option<Instant>> = Mutex::new(None);

static LOCAL_TIMER: Mutex<Option<ActiveTimer>> = Mutex::new(None);
static LOCAL_ALARM: Mutex<Option<ActiveAlarm>> = Mutex::new(None);
static ALARM_RINGING: AtomicBool = AtomicBool::new(false);
static ALARM_RINGING_KIND: Mutex<String> = Mutex::new(String::new());
static LAST_ALARM_CHIME: Mutex<Option<Instant>> = Mutex::new(None);
static ALARM_START_TIME: Mutex<Option<Instant>> = Mutex::new(None);
static ALICE_BUSY_FILE: &str = "/tmp/alice_busy";
static MIC_STATUS_FILE: &str = "/tmp/mic.status";
static FIFO_MIC_CONTROL: &str = "/tmp/mic.control";
static MIC_MUTED: AtomicBool = AtomicBool::new(false);
static LAST_MIC_MUTE_TOGGLE: Mutex<Option<Instant>> = Mutex::new(None);
static LAST_VOLUME_TOUCH: Mutex<Option<Instant>> = Mutex::new(None);

/// Fire-and-forget without leaking zombies: a tiny waiter owns the child.
fn spawn_reaped(cmd: &mut Command) {
    if let Ok(mut child) = cmd.spawn() {
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
}

fn set_hardware_mic_mute(mute: bool) {
    if let Some(sock) = find_pulse_socket() {
        let env_s = format!("unix:{sock}");
        let arg = if mute { "1" } else { "0" };
        let _ = Command::new("pactl")
            .env("PULSE_SERVER", &env_s)
            .args(["set-source-mute", "speex", arg])
            .status();
    }
}

fn play_sound_file(path: &str) {
    let sock = find_pulse_socket();
    let mut cmd = Command::new("paplay");
    cmd.args(["--device=master", path]);
    if let Some(ref s) = sock {
        cmd.env("PULSE_SERVER", format!("unix:{s}"));
    }
    spawn_reaped(&mut cmd);
}

fn apply_mic_mute(mute: bool, _announce: bool) {
    MIC_MUTED.store(mute, Ordering::SeqCst);
    let _ = std::fs::write(MIC_STATUS_FILE, if mute { "1\n" } else { "0\n" });
    set_hardware_mic_mute(mute);
    let _ = std::fs::write(WAKE_TRIGGER_FILE, "");

    if mute {
        set_ring("mute");
    } else {
        set_ring("unmute");
    }
}

fn spawn_mic_control_listener() {
    tokio::spawn(async move {
        let _ = std::fs::remove_file(FIFO_MIC_CONTROL);
        let _ = Command::new("mkfifo").arg(FIFO_MIC_CONTROL).status();
        let _ = Command::new("chmod").args(["666", FIFO_MIC_CONTROL]).status();

        loop {
            if let Ok(content) = tokio::fs::read_to_string(FIFO_MIC_CONTROL).await {
                let trimmed = content.trim();
                match trimmed {
                    "mute" | "off" | "1" => {
                        println!("[Mic Control] Получена команда MUTE через FIFO.");
                        apply_mic_mute(true, true);
                    }
                    "unmute" | "on" | "0" => {
                        println!("[Mic Control] Получена команда UNMUTE через FIFO.");
                        apply_mic_mute(false, true);
                    }
                    "toggle" => {
                        let cur = MIC_MUTED.load(Ordering::SeqCst);
                        println!("[Mic Control] Получена команда TOGGLE через FIFO.");
                        apply_mic_mute(!cur, true);
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

fn set_alice_busy(busy: bool) {
    if busy {
        let _ = std::fs::File::create(ALICE_BUSY_FILE);
    } else {
        let _ = std::fs::remove_file(ALICE_BUSY_FILE);
    }
}
const FIFO_MUSIC_CONTROL: &str = "/run/capsula-yandex/control";
const INPUT_EVENT_DEV: &str = "/dev/input/event0";

const SAMPLE_RATE_PLAY: u32 = 48000;
const SAMPLE_RATE_REC: u32 = 16000;
const CHUNK_SAMPLES: usize = 320; // 20ms at 16kHz for lightning-fast real-time response
const CHUNK_BYTES: usize = CHUNK_SAMPLES * 2; // 16-bit mono (640 bytes)

// ---------------------------------------------------------------------------
// 1. PulseAudio Socket Finder
// ---------------------------------------------------------------------------
fn find_pulse_socket() -> Option<String> {
    if let Ok(content) = fs::read_to_string("/proc/net/unix") {
        for line in content.lines() {
            if let Some(idx) = line.find("/tmp/pulse-") {
                let path = &line[idx..];
                if path.ends_with("/native") && Path::new(path).exists() {
                    return Some(path.to_string());
                }
            }
        }
    }
    if let Ok(entries) = fs::read_dir("/tmp") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("pulse-") {
                let sock = format!("/tmp/{}/native", name);
                if Path::new(&sock).exists() {
                    return Some(sock);
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// 2. Credentials
// ---------------------------------------------------------------------------
fn find_yandex_token() -> Option<String> {
    let candidate_paths = [
        "/etc/alice/yandex_credentials.json",
        "/data/capsula-yandex/credentials.json",
        "/data/capsula-hub/credentials.json",
        "/data/yandex_credentials.json",
        "/tmp/yandex_credentials.json",
    ];

    for p in &candidate_paths {
        if let Ok(content) = fs::read_to_string(p) {
            if let Ok(v) = serde_json::from_str::<Value>(&content) {
                if let Some(tok) = v.get("access_token").and_then(|t| t.as_str()) {
                    if !tok.trim().is_empty() {
                        return Some(tok.trim().to_string());
                    }
                }
                if let Some(tok) = v.get("token").and_then(|t| t.as_str()) {
                    if !tok.trim().is_empty() {
                        return Some(tok.trim().to_string());
                    }
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// 3. Hardware LED Ring & Screen
// ---------------------------------------------------------------------------
fn set_ring(cmd: &str) {
    if let Ok(mut log) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/ring_calls.log")
    {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = writeln!(log, "{ts} {cmd}");
    }
    for _ in 0..4 {
        if let Ok(mut f) = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(FIFO_RING)
        {
            let _ = writeln!(f, "{}", cmd);
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// 3.5 Hardware Display (Clock) & Volume Management
// Physical Pinout on VK Capsule Mini:
// D1: a=2, b=29, c=22, d=18, e=23, f=30, g=25
// D2: a=27, b=26, c=20, d=19, e=24, f=28, g=21
// D3: a=1, b=3, c=8, d=6, e=7, f=5, g=4
// D4: a=11, b=13, c=12, d=15, e=14, f=9, g=10
// Colon: top=17, bottom=16
// ---------------------------------------------------------------------------
struct DigitPins {
    a: usize,
    b: usize,
    c: usize,
    d: usize,
    e: usize,
    f: usize,
    g: usize,
}

const D1: DigitPins = DigitPins { a: 2, b: 29, c: 22, d: 18, e: 23, f: 30, g: 25 };
const D2: DigitPins = DigitPins { a: 27, b: 26, c: 20, d: 19, e: 24, f: 28, g: 21 };
const D3: DigitPins = DigitPins { a: 1, b: 3, c: 8, d: 6, e: 7, f: 5, g: 4 };
const D4: DigitPins = DigitPins { a: 11, b: 13, c: 12, d: 15, e: 14, f: 9, g: 10 };
const COLON_TOP: usize = 17;
const COLON_BOTTOM: usize = 16;

fn get_digit_mask(d: u32) -> u8 {
    match d {
        0 => 0b00111111, // a b c d e f
        1 => 0b00000110, // b c
        2 => 0b01011011, // a b d e g
        3 => 0b01001111, // a b c d g
        4 => 0b01100110, // b c f g
        5 => 0b01101101, // a c d f g
        6 => 0b01111101, // a c d e f g
        7 => 0b00000111, // a b c
        8 => 0b01111111, // a b c d e f g
        9 => 0b01101111, // a b c d f g
        _ => 0,
    }
}

fn apply_digit_mask(pins: &DigitPins, mask: u8, leds: &mut [bool; 31]) {
    if (mask & 0x01) != 0 { leds[pins.a] = true; }
    if (mask & 0x02) != 0 { leds[pins.b] = true; }
    if (mask & 0x04) != 0 { leds[pins.c] = true; }
    if (mask & 0x08) != 0 { leds[pins.d] = true; }
    if (mask & 0x10) != 0 { leds[pins.e] = true; }
    if (mask & 0x20) != 0 { leds[pins.f] = true; }
    if (mask & 0x40) != 0 { leds[pins.g] = true; }
}

static SCREEN_OVERLAY: std::sync::Mutex<Option<(u32, Instant)>> = std::sync::Mutex::new(None);
static CURRENT_VOLUME: AtomicU32 = AtomicU32::new(20);

fn show_volume_overlay(vol: u32) {
    if let Ok(mut g) = SCREEN_OVERLAY.lock() {
        *g = Some((vol, Instant::now() + Duration::from_millis(1800)));
    }
}

fn apply_volume(vol: u32) {
    let sock = find_pulse_socket();
    let mut cmd = Command::new("pactl");
    if let Some(ref s) = sock {
        cmd.env("PULSE_SERVER", format!("unix:{s}"));
    }
    cmd.args(["set-sink-volume", "master", &format!("{vol}%")]);
    spawn_reaped(&mut cmd);
    // Do not unmute here. A muted capsule must stay silent at night.
}

fn set_ring_volume(vol: u32) {
    set_ring(&format!("volume {vol}"));
}

fn read_als_raw() -> Option<f32> {
    if let Ok(s) = fs::read_to_string("/sys/bus/iio/devices/iio:device0/in_illuminance_clear_raw") {
        s.trim().parse::<f32>().ok()
    } else {
        None
    }
}

fn als_to_brightness(raw: f32) -> u8 {
    let r = raw.max(0.0);
    if r <= 2.0 {
        15
    } else if r <= 10.0 {
        (15.0 + (r - 2.0) * 1.5) as u8
    } else {
        let b = 27.0 + 38.0 * ((1.0 + (r - 10.0) / 15.0).log2());
        b.clamp(15.0, 240.0) as u8
    }
}

fn spawn_clock_daemon() {
    std::thread::Builder::new().name("capsula-clock".into()).spawn(move || {
        let mut last_state = [false; 31];
        let mut last_brightness = 0u8;
        let mut smoothed_als: f32 = 10.0;
        let mut current_brightness: f32 = 30.0;

        loop {
            let now = Instant::now();
            let mut active_leds = [false; 31];

            let overlay = {
                if let Ok(mut g) = SCREEN_OVERLAY.lock() {
                    if let Some((vol, deadline)) = *g {
                        if now < deadline {
                            Some(vol)
                        } else {
                            *g = None;
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            let brightness: u8;
            if let Some(vol) = overlay {
                brightness = 140;
                let tens = (vol / 10).min(9);
                let ones = vol % 10;
                if tens > 0 {
                    apply_digit_mask(&D3, get_digit_mask(tens), &mut active_leds);
                }
                apply_digit_mask(&D4, get_digit_mask(ones), &mut active_leds);
            } else {
                // Hardware Auto-brightness from ROHM BH1730 ALS sensor (I2C 0x29)
                let target_brightness: f32 = if let Some(raw) = read_als_raw() {
                    smoothed_als = 0.85 * smoothed_als + 0.15 * raw;
                    als_to_brightness(smoothed_als) as f32
                } else {
                    110.0
                };
                current_brightness += (target_brightness - current_brightness) * 0.25;
                brightness = current_brightness.round().clamp(15.0, 240.0) as u8;

                // Приоритет 1: Таймер или будильник звенит -> мигаем 00:00
                if ALARM_RINGING.load(Ordering::SeqCst) {
                    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
                    if (now_ms / 500) % 2 == 0 {
                        apply_digit_mask(&D1, get_digit_mask(0), &mut active_leds);
                        apply_digit_mask(&D2, get_digit_mask(0), &mut active_leds);
                        apply_digit_mask(&D3, get_digit_mask(0), &mut active_leds);
                        apply_digit_mask(&D4, get_digit_mask(0), &mut active_leds);
                        active_leds[COLON_TOP] = true;
                        active_leds[COLON_BOTTOM] = true;
                    }
                // Приоритет 2: Включен обратный отсчет таймера -> отображаем ММ:СС (или ЧЧ:ММ если > 1 часа)
                } else if let Some(rem_secs) = {
                    if let Ok(t_lock) = LOCAL_TIMER.lock() {
                        t_lock.as_ref().map(|t| {
                            let cur = Instant::now();
                            if t.deadline > cur {
                                t.deadline.duration_since(cur).as_secs()
                            } else {
                                0
                            }
                        })
                    } else {
                        None
                    }
                } {
                    let (d1, d2, d3, d4) = if rem_secs >= 3600 {
                        let h = (rem_secs / 3600) as u32;
                        let m = ((rem_secs % 3600) / 60) as u32;
                        (h / 10, h % 10, m / 10, m % 10)
                    } else {
                        let m = (rem_secs / 60) as u32;
                        let s = (rem_secs % 60) as u32;
                        (m / 10, m % 10, s / 10, s % 10)
                    };

                    if d1 > 0 {
                        apply_digit_mask(&D1, get_digit_mask(d1), &mut active_leds);
                    }
                    apply_digit_mask(&D2, get_digit_mask(d2), &mut active_leds);
                    apply_digit_mask(&D3, get_digit_mask(d3), &mut active_leds);
                    apply_digit_mask(&D4, get_digit_mask(d4), &mut active_leds);

                    active_leds[COLON_TOP] = true;
                    active_leds[COLON_BOTTOM] = true;
                // Приоритет 3: Обычные часы (Текущее время ЧЧ:ММ с мигающим двоеточием)
                } else {
                    let time_secs = unsafe {
                        let mut t: libc::time_t = 0;
                        libc::time(&mut t);
                        let mut tm: libc::tm = std::mem::zeroed();
                        libc::localtime_r(&t, &mut tm);
                        (tm.tm_hour as u32, tm.tm_min as u32, tm.tm_sec as u32)
                    };
                    let (hour, min, sec) = time_secs;

                    let d1 = hour / 10;
                    let d2 = hour % 10;
                    let d3 = min / 10;
                    let d4 = min % 10;

                    if d1 > 0 {
                        apply_digit_mask(&D1, get_digit_mask(d1), &mut active_leds);
                    }
                    apply_digit_mask(&D2, get_digit_mask(d2), &mut active_leds);
                    apply_digit_mask(&D3, get_digit_mask(d3), &mut active_leds);
                    apply_digit_mask(&D4, get_digit_mask(d4), &mut active_leds);

                    if sec % 2 == 0 {
                        active_leds[COLON_TOP] = true;
                        active_leds[COLON_BOTTOM] = true;
                    }
                }
            }

            for i in 1..=30 {
                let is_on = active_leds[i];
                if is_on != last_state[i] || brightness != last_brightness {
                    let val_str = if is_on { format!("{brightness}") } else { "0".to_string() };
                    let _ = fs::write(format!("/sys/class/leds/clock-led{i}/brightness"), &val_str);
                    last_state[i] = is_on;
                }
            }
            last_brightness = brightness;

            std::thread::sleep(Duration::from_millis(250));
        }
    }).expect("failed to spawn clock daemon");
}

// ---------------------------------------------------------------------------
// 4. Audio Output (WAV + PulseAudio master sink)
// ---------------------------------------------------------------------------
fn build_wav_header(pcm_len: usize, sample_rate: u32, channels: u16) -> [u8; 44] {
    let mut hdr = [0u8; 44];
    let data_len = pcm_len as u32;
    let riff_len = 36 + data_len;
    let byte_rate = sample_rate * (channels as u32) * 2;
    let block_align = channels * 2;

    hdr[0..4].copy_from_slice(b"RIFF");
    hdr[4..8].copy_from_slice(&riff_len.to_le_bytes());
    hdr[8..12].copy_from_slice(b"WAVE");
    hdr[12..16].copy_from_slice(b"fmt ");
    hdr[16..20].copy_from_slice(&16u32.to_le_bytes());
    hdr[20..22].copy_from_slice(&1u16.to_le_bytes());
    hdr[22..24].copy_from_slice(&channels.to_le_bytes());
    hdr[24..28].copy_from_slice(&sample_rate.to_le_bytes());
    hdr[28..32].copy_from_slice(&byte_rate.to_le_bytes());
    hdr[32..34].copy_from_slice(&block_align.to_le_bytes());
    hdr[34..36].copy_from_slice(&16u16.to_le_bytes());
    hdr[36..40].copy_from_slice(b"data");
    hdr[40..44].copy_from_slice(&data_len.to_le_bytes());
    hdr
}

static PLAYBACK_PID: AtomicU32 = AtomicU32::new(0);
/// Момент, когда фоновый pacat реально закончил играть (0 = ещё играет).
static PLAYBACK_DONE_AT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Играет ли ещё Алиса прямо сейчас.
fn speech_still_playing() -> bool {
    PLAYBACK_PID.load(Ordering::SeqCst) > 0
}

fn stop_speech() {
    let pid = PLAYBACK_PID.swap(0, Ordering::SeqCst);
    if pid > 0 {
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGKILL);
        }
    }
    PLAYBACK_DONE_AT_MS.store(now_ms(), Ordering::SeqCst);
}

fn play_speech_pcm(pcm_bytes: &[u8], sample_rate: u32) -> Duration {
    stop_speech();
    if pcm_bytes.is_empty() {
        println!("[TTS] Пустой PCM — нечего играть");
        return Duration::ZERO;
    }

    let sock = find_pulse_socket();
    println!(
        "[TTS] Воспроизведение {} байт @ {} Hz через pacat, socket={:?}",
        pcm_bytes.len(),
        sample_rate,
        sock
    );

    let mut cmd = Command::new("pacat");
    cmd.args([
        "--playback",
        "--device=master",
        &format!("--rate={sample_rate}"),
        "--channels=1",
        "--format=s16le",
    ]);
    if let Some(ref s) = sock {
        cmd.env("PULSE_SERVER", format!("unix:{s}"));
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let pcm_vec = pcm_bytes.to_vec();
    std::thread::spawn(move || {
        if let Ok(mut child) = cmd.spawn() {
            let pid = child.id();
            PLAYBACK_PID.store(pid, Ordering::SeqCst);
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(&pcm_vec);
                let _ = stdin.flush();
                drop(stdin);
            }
            let _ = child.wait();
            let _ = PLAYBACK_PID.compare_exchange(pid, 0, Ordering::SeqCst, Ordering::SeqCst);
        }
    });

    let duration_secs = (pcm_bytes.len() as f32) / ((sample_rate as f32) * 2.0);
    Duration::from_secs_f32(duration_secs)
}

/// Общий стример PCM в pacat. Возвращает Sender — пишем чанки по мере прихода.
/// pacat живёт в отдельном потоке, поэтому главный цикл НЕ блокируется:
/// пока Алиса говорит, мы продолжаем читать микрофон и можем её перебить.
fn spawn_speech_stream(sample_rate: u32) -> Option<std::sync::mpsc::Sender<Vec<u8>>> {
    stop_speech();
    let sock = find_pulse_socket();

    let mut cmd = Command::new("pacat");
    cmd.args([
        "--playback",
        "--device=master",
        &format!("--rate={sample_rate}"),
        "--channels=1",
        "--format=s16le",
        // Низкая задержка: 80 мс буфера вместо дефолтных ~2 с.
        "--latency-msec=80",
    ]);
    if let Some(ref s) = sock {
        cmd.env("PULSE_SERVER", format!("unix:{s}"));
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = cmd.spawn().ok()?;
    let pid = child.id();
    PLAYBACK_PID.store(pid, Ordering::SeqCst);
    PLAYBACK_DONE_AT_MS.store(0, Ordering::SeqCst);
    let mut stdin = child.stdin.take()?;

    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        while let Ok(chunk) = rx.recv() {
            if stdin.write_all(&chunk).is_err() {
                break;
            }
            let _ = stdin.flush();
        }
        drop(stdin);
        let _ = child.wait();
        PLAYBACK_DONE_AT_MS.store(now_ms(), Ordering::SeqCst);
        let _ = PLAYBACK_PID.compare_exchange(pid, 0, Ordering::SeqCst, Ordering::SeqCst);
    });

    println!("[TTS] Потоковое воспроизведение запущено (pacat PID={pid}, latency=80ms)");
    Some(tx)
}


fn play_chime() {
    // Звук активации отключен (беззвучный режим активации)
}

fn pcm_duration(bytes: usize, sample_rate: u32) -> Duration {
    if sample_rate == 0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f32((bytes as f32) / ((sample_rate as f32) * 2.0))
}

// ---------------------------------------------------------------------------
// 5. Music Manager (capsula-yandex FIFO bridge)
// ---------------------------------------------------------------------------
fn send_music_cmd(cmd: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(FIFO_MUSIC_CONTROL)
    {
        let _ = writeln!(f, "{}", cmd);
    }
}

/// Named internet radio: search a public station directory, then stream the
/// resolved URL through capsula-audio -> paplay. No Yandex "wave" involved.
async fn play_named_radio(name: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let response = match client
        .get("https://de1.api.radio-browser.info/json/stations/search")
        .query(&[
            ("name", name),
            ("hidebroken", "true"),
            // Our decoder handles MP3 only. Radio Vanya's main stream is AAC+,
            // so without this filter the station is found but stays silent.
            ("codec", "MP3"),
            ("order", "clickcount"),
            ("reverse", "true"),
            ("limit", "10"),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            println!("[Radio] Поиск станции «{name}» недоступен: {}", e.to_string());
            return false;
        }
    };
    let value: serde_json::Value = match response.json().await {
        Ok(v) => v,
        Err(_) => return false,
    };
    let mut chosen: Option<(String, String)> = None;
    let mut fallback: Option<(String, String)> = None;
    if let Some(items) = value.as_array() {
        for item in items {
            let stream = item
                .get("url_resolved")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
                .or_else(|| item.get("url").and_then(serde_json::Value::as_str))
                .unwrap_or("");
            if !stream.starts_with("http://") && !stream.starts_with("https://") {
                continue;
            }
            let title = item
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(name)
                .to_string();
            let candidate = (title, stream.to_string());
            let bitrate = item.get("bitrate").and_then(serde_json::Value::as_u64).unwrap_or(0);
            if bitrate == 0 {
                if fallback.is_none() {
                    fallback = Some(candidate);
                }
                continue;
            }
            chosen = Some(candidate);
            break;
        }
    }
    if chosen.is_none() {
        chosen = fallback;
    }
    let (title, stream) = match chosen {
        Some(v) => v,
        None => {
            println!("[Radio] Станция «{name}» не найдена в каталоге.");
            return false;
        }
    };
    println!("[Radio] Найдена станция: {title}");
    println!("[Radio] Поток: {stream}");

    send_music_cmd("stop");
    let _ = Command::new("killall").args(["-9", "capsula-audio", "paplay"]).status();
    let safe_stream = stream.replace('\'', "");
    let script = format!(
        "/usr/bin/capsula-audio --gain 0 --fade-in 250 '{safe_stream}' | /usr/bin/paplay --latency-msec=80"
    );
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(script);
    if let Some(sock) = find_pulse_socket() {
        cmd.env("PULSE_SERVER", format!("unix:{sock}"));
    }
    spawn_reaped(&mut cmd);
    true
}

fn music_sink_input_ids_script(volume: u32) -> String {
    // Only music streams: Yandex player and paplay used by radio/TTS.
    // pacat (Alice speech) must stay at full volume.
    format!(r#"
SOCK=$(grep -o '/tmp/pulse-[^/]*/native' /proc/net/unix | head -n 1)
[ -n "$SOCK" ] || exit 0
CURRENT=""
MATCH=0
dump_input() {{
  if [ -n "$CURRENT" ] && [ "$MATCH" -eq 1 ]; then echo "$CURRENT"; fi
}}
pactl --server=unix:$SOCK list sink-inputs 2>/dev/null | while IFS= read -r LINE; do
  case "$LINE" in
    "Sink Input #"*) dump_input; CURRENT=${{LINE#Sink Input #}}; MATCH=0 ;;
    *'application.name = "yandex-player"'*) MATCH=1 ;;
    *'application.name = "paplay"'*) MATCH=1 ;;
  esac
done
dump_input | while read -r ID; do
  [ -n "$ID" ] && pactl --server=unix:$SOCK set-sink-input-volume "$ID" {volume} 2>/dev/null
done
"#)
}

fn duck_music() {
    // While listening, music drops to 10% of its own stream volume.
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(music_sink_input_ids_script(6554));
    spawn_reaped(&mut cmd);
}

fn unduck_music() {
    // Restore full stream volume after command/speech.
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(music_sink_input_ids_script(65536));
    spawn_reaped(&mut cmd);
}

// ---------------------------------------------------------------------------
// 6. Uniproxy Client (Independent Sessions for 100% Reliability)
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct UniproxySession {
    token: String,
}

impl UniproxySession {
    fn new(token: String) -> Self {
        Self { token }
    }

    async fn connect_and_sync(&self) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
        let (mut ws, _) = connect_async(UNIPROXY_URL).await
            .context("Не удалось подключиться к Uniproxy")?;

        let dev_uuid = uuid_v4_hex();
        let sync_msg = json!({
            "event": {
                "header": {
                    "namespace": "System",
                    "name": "SynchronizeState",
                    "messageId": uuid_v4()
                },
                "payload": {
                    "uuid": dev_uuid,
                    "lang": "ru-RU",
                    "voice": "shitova.us",
                    "clientTime": iso_now(),
                    "timezone": "Europe/Moscow",
                    "auth_token": self.token
                }
            }
        });

        ws.send(Message::Text(sync_msg.to_string().into())).await?;
        // Ждём SynchronizeStateResponse: если начать Recognize раньше,
        // сервер отвечает пустыми Result (проверено).
        let _ = tokio::time::timeout(Duration::from_millis(2000), ws.next()).await;
        Ok(ws)
    }

    async fn recognize_pcm_stream(
        &self,
        audio_chunks: &[Vec<u8>],
        pre_ws: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    ) -> Result<String> {
        let mut ws = match pre_ws {
            Some(w) => w,
            None => self.connect_and_sync().await?,
        };
        let msg_id = uuid_v4();
        let stream_id = 1u32;

        let asr_req = json!({
            "event": {
                "header": {
                    "namespace": "ASR",
                    "name": "Recognize",
                    "messageId": msg_id,
                    "streamId": stream_id
                },
                "payload": {
                    "lang": "ru-RU",
                    "topic": "dialog-general",
                    "format": "audio/x-pcm;bit=16;rate=16000",
                    "punctuation": false
                }
            }
        });

        ws.send(Message::Text(asr_req.to_string().into())).await?;

        // Group 20ms chunks into 100ms packets (3200 bytes) for fast upload
        let mut combined_buf = Vec::with_capacity(3200);
        for chunk in audio_chunks {
            combined_buf.extend_from_slice(chunk);
            if combined_buf.len() >= 3200 {
                let mut packet = Vec::with_capacity(4 + combined_buf.len());
                packet.extend_from_slice(&stream_id.to_be_bytes());
                packet.extend_from_slice(&combined_buf);
                ws.send(Message::Binary(packet.into())).await?;
                combined_buf.clear();
                sleep(Duration::from_millis(5)).await;
            }
        }
        if !combined_buf.is_empty() {
            let mut packet = Vec::with_capacity(4 + combined_buf.len());
            packet.extend_from_slice(&stream_id.to_be_bytes());
            packet.extend_from_slice(&combined_buf);
            ws.send(Message::Binary(packet.into())).await?;
        }

        // Send 250ms trailing silence padding for acoustic model completion
        let silence = vec![0u8; (SAMPLE_RATE_REC as usize) * 2 * 25 / 100];
        let mut packet = Vec::with_capacity(4 + silence.len());
        packet.extend_from_slice(&stream_id.to_be_bytes());
        packet.extend_from_slice(&silence);
        let _ = ws.send(Message::Binary(packet.into())).await;

        // CloseStream
        let close_req = json!({
            "event": {
                "header": {
                    "namespace": "StreamControl",
                    "name": "CloseStream",
                    "messageId": uuid_v4()
                },
                "payload": {
                    "streamId": stream_id
                }
            }
        });
        let _ = ws.send(Message::Text(close_req.to_string().into())).await;

        // Wait for final result
        let mut final_text = String::new();
        let mut result_count = 0usize;
        let deadline = Instant::now() + Duration::from_millis(2200);

        while Instant::now() < deadline {
            let wait_dur = if final_text.is_empty() { Duration::from_millis(800) } else { Duration::from_millis(200) };
            match tokio::time::timeout(wait_dur, ws.next()).await {
                Ok(Some(Ok(Message::Text(txt)))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(txt.as_str()) {
                        if v.pointer("/directive/header/name").and_then(|n| n.as_str()) == Some("Result") {
                            result_count += 1;
                            if let Some(recs) = v.pointer("/directive/payload/recognition").and_then(|r| r.as_array()) {
                                for r in recs {
                                    if let Some(norm) = r.get("normalized").and_then(|n| n.as_str()) {
                                        if !norm.is_empty() {
                                            final_text = norm.to_string();
                                            break;
                                        }
                                    }
                                    if let Some(words) = r.get("words").and_then(|w| w.as_array()) {
                                        let vals: Vec<String> = words.iter()
                                            .filter_map(|w| w.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()))
                                            .collect();
                                        if !vals.is_empty() {
                                            final_text = vals.join(" ");
                                            break;
                                        }
                                    }
                                }
                            }
                            if is_alice_wake(&final_text) {
                                break;
                            }
                            if v.pointer("/directive/payload/endOfUtt").and_then(|e| e.as_bool()) == Some(true) {
                                if !final_text.is_empty() {
                                    break;
                                }
                            }
                        }
                        // Завершение потока распознавания сервером — выходим сразу!
                        if v.pointer("/streamcontrol/action").and_then(|a| a.as_i64()) == Some(0) {
                            break;
                        }
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    eprintln!("[ASR Error in stream]: {e}");
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    if !final_text.is_empty() {
                        break;
                    }
                }
            }
        }

        let _ = ws.close(None).await;
        if final_text.is_empty() {
            eprintln!("[ASR] Пустой результат (Result-директив получено: {result_count})");
        }
        Ok(final_text)
    }

    /// Полный конвейер «вопрос → Vins → TTS» в ОДНОМ WebSocket-соединении.
    /// Аудио отдаётся потоком в `audio_tx` по мере прихода: воспроизведение
    /// начинается на первом чанке, не дожидаясь конца синтеза.
    /// Возвращает (текст ответа, длительность PCM, музыка-флаг, пауза-флаг, track_id).
    async fn ask_vins_and_stream(
        &self,
        text: &str,
        audio_tx: Option<std::sync::mpsc::Sender<Vec<u8>>>,
        mut pre_ws: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    ) -> Result<(String, Duration, bool, bool, Option<String>)> {
        let mut ws = match pre_ws.take() {
            Some(w) => w,
            None => self.connect_and_sync().await?,
        };
        let t_vins_start = Instant::now();
        let mut tts_sent_at: Option<Instant> = None;
        let mut first_audio_logged = false;
        let mut last_audio_at: Option<Instant> = None;
        let dev_uuid = uuid_v4_hex();

        // 1. Send Vins TextInput
        let vins_payload = json!({
            "request": {
                "voice_session": false,
                "event": {
                    "type": "text_input",
                    "text": text
                },
                "additional_options": {
                    "oauth_token": self.token
                },
                // device_state нужен Vins, чтобы отвечать как станция
                // (иначе часть вопросов уходит в общий шаблон).
                "device_state": {
                    "sound_level": 5,
                    "sound_max_level": 10,
                    "is_muted": false,
                    "music": { "player_state": "Stopped" }
                }
            },
            "application": {
                "app_id": "ru.yandex.quasar.mini2",
                "app_version": "2.0",
                "os_version": "Linux",
                "platform": "linux",
                "uuid": dev_uuid,
                "lang": "ru-RU",
                "client_time": iso_now(),
                "timezone": "Europe/Moscow",
                "timestamp": current_timestamp_secs().to_string()
            }
        });

        ws.send(Message::Text(json!({
            "event": {
                "header": {
                    "namespace": "Vins",
                    "name": "TextInput",
                    "messageId": uuid_v4()
                },
                "payload": vins_payload
            }
        }).to_string().into())).await?;

        // 2. Wait for VinsResponse and immediately pipeline TTS.Generate on the SAME WebSocket
        let mut reply_text = String::new();
        let mut has_music = false;
        let mut has_pause = false;
        let mut track_id: Option<String> = None;
        let mut total_pcm = 0usize;
        let mut tts_requested = false;
        let mut got_audio = false;
        // Генеративные ответы (YandexGPT) формируются до 9 с, затем до 15 с на поток TTS.
        let mut deadline = Instant::now() + Duration::from_millis(9000);

        while Instant::now() < deadline {
            // TTS-поток часто не присылает streamcontrol сразу после последнего
            // аудиочанка. Не ждём 9–15 с: обрываем через 1200 мс тишины в аудио.
            if tts_requested {
                if let Some(ts) = last_audio_at {
                    if ts.elapsed() > Duration::from_millis(1200) {
                        break;
                    }
                }
                // TTS запрос ушёл, но ни одного аудиочанка нет — не висим 15 с.
                if !got_audio {
                    if let Some(ts) = tts_sent_at {
                        if ts.elapsed() > Duration::from_millis(4500) {
                            eprintln!("[Latency] TTS timeout: нет аудио за 4500 мс");
                            break;
                        }
                    }
                }
            }
            let step_timeout = if tts_requested {
                Duration::from_millis(1500)
            } else {
                Duration::from_millis(1200)
            };

            match tokio::time::timeout(step_timeout, ws.next()).await {
                Ok(Some(Ok(Message::Text(txt)))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(txt.as_str()) {
                        let dname = v.pointer("/directive/header/name").and_then(|n| n.as_str()).unwrap_or("");
                        if !tts_requested && dname == "VinsResponse" {
                            let resp = v.pointer("/directive/payload/response");
                            if let Some(cards) = resp.and_then(|r| r.get("cards")).and_then(|c| c.as_array()) {
                                for card in cards {
                                    if let Some(t) = card.get("text").and_then(|s| s.as_str()) {
                                        reply_text = t.to_string();
                                        break;
                                    }
                                }
                            }
                            if reply_text.is_empty() {
                                if let Some(t) = resp.and_then(|r| r.pointer("/card/text")).and_then(|s| s.as_str()) {
                                    reply_text = t.to_string();
                                }
                            }
                            if reply_text.is_empty() {
                                if let Some(t) = resp.and_then(|r| r.pointer("/voice_response/output_speech/text")).and_then(|s| s.as_str()) {
                                    reply_text = t.to_string();
                                }
                            }
                            if let Some(dirs) = resp.and_then(|r| r.get("directives")).and_then(|d| d.as_array()) {
                                for dir_item in dirs {
                                    let dn = dir_item.get("name").and_then(|n| n.as_str()).unwrap_or("");
                                    let dp = dir_item.get("payload");
                                    if dn == "music_play" {
                                        has_music = true;
                                        track_id = dp.and_then(|p| p.get("first_track_id")).and_then(|id| id.as_str()).map(|s| s.to_string());
                                    } else if dn == "player_pause" || dn == "player_stop" {
                                        has_pause = true;
                                    }
                                }
                            }

                            if reply_text.is_empty() {
                                reply_text = "Я вас слушаю!".to_string();
                            }
                            // Vins иногда отдаёт служебный отказ вместо ответа.
                            // Не озвучиваем его: лучше естественный короткий ответ.
                            if reply_text.contains("Не придумала") || reply_text.contains("не придумала") {
                                println!("[Vins] Получен служебный отказ, используем запасную фразу.");
                                reply_text = "Я вас слушаю. Спросите ещё раз чуть иначе.".to_string();
                            }

                            // Пайплайн: шлём TTS.Generate в тот же WebSocket без нового TLS-хендшейка
                            let tts_req = json!({
                                "event": {
                                    "header": {
                                        "namespace": "TTS",
                                        "name": "Generate",
                                        "messageId": uuid_v4()
                                    },
                                    "payload": {
                                        "voice": "shitova.us",
                                        "lang": "ru-RU",
                                        "format": "audio/x-pcm;bit=16;rate=48000",
                                        "emotion": "neutral",
                                        "quality": "Normal",
                                        "text": reply_text,
                                        "auth_token": self.token
                                    }
                                }
                            });
                            ws.send(Message::Text(tts_req.to_string().into())).await?;
                            tts_requested = true;
                            tts_sent_at = Some(Instant::now());
                            deadline = Instant::now() + Duration::from_millis(15000);
                        } else if v.pointer("/streamcontrol/action").and_then(|a| a.as_i64()) == Some(0) {
                            if tts_requested {
                                break;
                            } else {
                                if reply_text.is_empty() {
                                    reply_text = "Я вас слушаю!".to_string();
                                }
                                let tts_req = json!({
                                    "event": {
                                        "header": {
                                            "namespace": "TTS",
                                            "name": "Generate",
                                            "messageId": uuid_v4()
                                        },
                                        "payload": {
                                            "voice": "shitova.us",
                                            "lang": "ru-RU",
                                            "format": "audio/x-pcm;bit=16;rate=48000",
                                            "emotion": "neutral",
                                            "quality": "Normal",
                                            "text": reply_text,
                                            "auth_token": self.token
                                        }
                                    }
                                });
                                ws.send(Message::Text(tts_req.to_string().into())).await?;
                                tts_requested = true;
                                tts_sent_at = Some(Instant::now());
                                deadline = Instant::now() + Duration::from_millis(15000);
                            }
                        }
                    }
                }
                Ok(Some(Ok(Message::Binary(bin)))) => {
                    if bin.len() > 4 {
                        let chunk = &bin[4..];
                        total_pcm += chunk.len();
                        got_audio = true;
                        last_audio_at = Some(Instant::now());
                        if !first_audio_logged {
                            first_audio_logged = true;
                            if let Some(ts) = tts_sent_at {
                                println!("[Latency] TTS first chunk: {} ms (Vins+WS total: {} ms)",
                                         ts.elapsed().as_millis(), t_vins_start.elapsed().as_millis());
                            }
                        }
                        if let Some(ref tx) = audio_tx {
                            // Не блокируем сетевой цикл на диске/stdin.
                            let _ = tx.send(chunk.to_vec());
                        }
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    eprintln!("[Vins+TTS Error in stream]: {e}");
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    // Тишина после начала аудио = конец синтеза.
                    if tts_requested && got_audio {
                        break;
                    }
                }
            }
        }

        let _ = ws.close(None).await;
        if tts_requested && !got_audio {
            return Err(anyhow::anyhow!("TTS не вернул аудио"));
        }
        if reply_text.is_empty() {
            reply_text = "Я вас слушаю!".to_string();
        }
        Ok((reply_text, pcm_duration(total_pcm, SAMPLE_RATE_PLAY), has_music, has_pause, track_id))
    }

    async fn synthesize_tts_pcm(
        &self,
        text: &str,
        pre_ws: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    ) -> Result<Vec<u8>> {
        let mut ws = match pre_ws {
            Some(w) => w,
            None => self.connect_and_sync().await?,
        };
        let msg_id = uuid_v4();
        let tts_req = json!({
            "event": {
                "header": {
                    "namespace": "TTS",
                    "name": "Generate",
                    "messageId": msg_id
                },
                "payload": {
                    "voice": "shitova.us",
                    "lang": "ru-RU",
                    "format": "audio/x-pcm;bit=16;rate=48000",
                    "emotion": "neutral",
                    "quality": "Normal",
                    "text": text,
                    "auth_token": self.token
                }
            }
        });

        ws.send(Message::Text(tts_req.to_string().into())).await?;

        let mut pcm = Vec::with_capacity(96000);
        // Длинные ответы (YandexGPT) не влезали в 4 с — обрывались на полуслове.
        let deadline = Instant::now() + Duration::from_millis(15000);

        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(1500), ws.next()).await {
                Ok(Some(Ok(Message::Binary(bin)))) => {
                    if bin.len() > 4 {
                        pcm.extend_from_slice(&bin[4..]);
                    }
                }
                Ok(Some(Ok(Message::Text(txt)))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(txt.as_str()) {
                        if v.pointer("/streamcontrol/action").and_then(|a| a.as_i64()) == Some(0) {
                            break;
                        }
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    eprintln!("[TTS Error in stream]: {e}");
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    if !pcm.is_empty() {
                        break;
                    }
                }
            }
        }

        let _ = ws.close(None).await;
        Ok(pcm)
    }
}

// ---------------------------------------------------------------------------
// 7. Helpers: UUID, ISO Date, RMS, Russian Plural
// ---------------------------------------------------------------------------
fn uuid_v4() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

fn uuid_v4_hex() -> String {
    uuid_v4().replace('-', "")
}

fn iso_now() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let rem = secs % 86400;
    let hours = rem / 3600;
    let mins = (rem % 3600) / 60;
    let s = rem % 60;
    format!("2026-11-09T{:02}:{:02}:{:02}Z", hours, mins, s)
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn compute_rms(samples: &[i16]) -> f32 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum_sq: f64 = samples.iter().map(|&s| (s as f64) * (s as f64)).sum();
    ((sum_sq / (samples.len() as f64)).sqrt()) as f32
}

// Keyboard clicks and knocks have a much higher peak/RMS ratio than speech.
fn compute_peak(samples: &[i16]) -> f32 {
    samples
        .iter()
        .map(|&s| if s < 0 { -(s as f32) } else { s as f32 })
        .fold(0.0f32, f32::max)
}

fn parse_russian_num(s: &str) -> Option<u64> {
    if let Ok(v) = s.parse::<u64>() {
        return Some(v);
    }
    match s {
        "один" | "одну" | "одна" | "одним" => Some(1),
        "два" | "две" | "двумя" => Some(2),
        "три" | "тремя" => Some(3),
        "четыре" | "четырьмя" => Some(4),
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
        "шестнадцать" => Some(16),
        "семнадцать" => Some(17),
        "восемнадцать" => Some(18),
        "девятнадцать" => Some(19),
        "двадцать" => Some(20),
        "тридцать" => Some(30),
        "сорок" => Some(40),
        "пятьдесят" => Some(50),
        "шестьдесят" => Some(60),
        _ => None,
    }
}

enum TimerAction {
    Set(u64, String),
    Status,
    Cancel,
}

fn parse_timer_command(cmd: &str) -> Option<TimerAction> {
    if !cmd.contains("таймер") && !cmd.contains("засеки") {
        return None;
    }
    if cmd.contains("отмен") || cmd.contains("сброс") || cmd.contains("удали") || cmd.contains("выключ") {
        return Some(TimerAction::Cancel);
    }
    if cmd.contains("сколько") || cmd.contains("осталось") || cmd.contains("статус") {
        return Some(TimerAction::Status);
    }
    if cmd.contains("полчаса") {
        return Some(TimerAction::Set(1800, "30 минут".to_string()));
    }
    if cmd.contains("полтора часа") {
        return Some(TimerAction::Set(5400, "полтора часа".to_string()));
    }

    let tokens: Vec<&str> = cmd.split_whitespace().collect();
    let mut total_secs: u64 = 0;
    let mut i = 0;
    while i < tokens.len() {
        if let Some(num) = parse_russian_num(tokens[i]) {
            if i + 1 < tokens.len() {
                let next_token = tokens[i + 1];
                if next_token.starts_with("сек") {
                    total_secs += num;
                    i += 2;
                    continue;
                } else if next_token.starts_with("мин") {
                    total_secs += num * 60;
                    i += 2;
                    continue;
                } else if next_token.starts_with("час") {
                    total_secs += num * 3600;
                    i += 2;
                    continue;
                }
            }
            total_secs += num * 60;
        }
        i += 1;
    }

    if total_secs > 0 {
        let label = if total_secs >= 60 {
            let m = total_secs / 60;
            plural_russian(m as u32, "минуту", "минуты", "минут")
        } else {
            plural_russian(total_secs as u32, "секунду", "секунды", "секунд")
        };
        Some(TimerAction::Set(total_secs, label))
    } else {
        None
    }
}

/// Feature 5: «напомни мне через 10 минут выключить плиту» -> (600, "выключить плиту").
fn parse_reminder_command(cmd: &str) -> Option<(u64, String)> {
    if !cmd.contains("напомни") && !cmd.contains("напоминани") {
        return None;
    }
    if cmd.contains("отмен") || cmd.contains("сброс") || cmd.contains("удали") {
        return None;
    }
    let tokens: Vec<&str> = cmd.split_whitespace().collect();
    let mut total_secs: u64 = 0;
    let mut consumed = vec![false; tokens.len()];
    let mut i = 0;
    while i < tokens.len() {
        if let Some(num) = parse_russian_num(tokens[i]) {
            let unit = if i + 1 < tokens.len() { tokens[i + 1] } else { "" };
            if unit.starts_with("сек") {
                total_secs += num;
                consumed[i] = true;
                consumed[i + 1] = true;
                i += 2;
                continue;
            } else if unit.starts_with("мин") {
                total_secs += num * 60;
                consumed[i] = true;
                consumed[i + 1] = true;
                i += 2;
                continue;
            } else if unit.starts_with("час") {
                total_secs += num * 3600;
                consumed[i] = true;
                consumed[i + 1] = true;
                i += 2;
                continue;
            }
        }
        i += 1;
    }
    if total_secs == 0 {
        return None;
    }
    let text: Vec<&str> = tokens
        .iter()
        .enumerate()
        .filter(|(idx, _)| !consumed[*idx])
        .map(|(_, t)| *t)
        .filter(|t| {
            !matches!(
                *t,
                "напомни" | "напомнить" | "напоминание" | "мне" | "через" | "и" | "да" | "пожалуйста"
            )
        })
        .collect();
    let what = if text.is_empty() {
        "что-то".to_string()
    } else {
        text.join(" ")
    };
    Some((total_secs, what))
}

enum AlarmAction {
    Set(u32, u32, String),
    Status,
    Cancel,
}

fn parse_alarm_command(cmd: &str) -> Option<AlarmAction> {
    if !cmd.contains("будильник") && !cmd.contains("разбуди") {
        return None;
    }
    if cmd.contains("отмен") || cmd.contains("сброс") || cmd.contains("удали") || cmd.contains("выключ") {
        return Some(AlarmAction::Cancel);
    }
    if cmd.contains("сколько") || cmd.contains("когда") || cmd.contains("статус") {
        return Some(AlarmAction::Status);
    }

    for part in cmd.split_whitespace() {
        if let Some((h_str, m_str)) = part.split_once(':') {
            if let (Ok(h), Ok(m)) = (h_str.parse::<u32>(), m_str.parse::<u32>()) {
                if h < 24 && m < 60 {
                    return Some(AlarmAction::Set(h, m, format!("{h:02}:{m:02}")));
                }
            }
        }
    }

    let is_pm = cmd.contains("вечер") || cmd.contains("дня");
    let is_am = cmd.contains("утр") || cmd.contains("ночи");

    let tokens: Vec<&str> = cmd.split_whitespace().collect();
    let mut nums: Vec<u32> = Vec::new();
    for token in tokens {
        let clean = token.trim_matches(|c: char| !c.is_alphanumeric());
        if let Some(n) = parse_russian_num(clean) {
            nums.push(n as u32);
        }
    }

    if !nums.is_empty() {
        let mut h = nums[0];
        if is_pm && h < 12 {
            h += 12;
        } else if is_am && h == 12 {
            h = 0;
        }
        let m = if nums.len() >= 2 { nums[1] } else { 0 };
        if h < 24 && m < 60 {
            return Some(AlarmAction::Set(h, m, format!("{h:02}:{m:02}")));
        }
    }
    None
}

fn get_local_h_m_s() -> (u32, u32, u32) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = now.as_secs() + 3 * 3600; // MSK UTC+3
    let h = ((total_secs % 86400) / 3600) as u32;
    let m = ((total_secs % 3600) / 60) as u32;
    let s = (total_secs % 60) as u32;
    (h, m, s)
}

fn calculate_alarm_diff_secs(target_h: u32, target_m: u32) -> u64 {
    let (cur_h, cur_m, cur_s) = get_local_h_m_s();
    let cur_total_secs = (cur_h * 3600 + cur_m * 60 + cur_s) as i64;
    let target_total_secs = (target_h * 3600 + target_m * 60) as i64;
    let mut diff = target_total_secs - cur_total_secs;
    if diff <= 0 {
        diff += 86400; // tomorrow
    }
    diff as u64
}

fn play_alarm_sound(kind: &str) {
    let sock = find_pulse_socket();
    let mut cmd = Command::new("paplay");
    let file = if kind == "alarm" {
        "/usr/share/sounds/alarm_ring.wav"
    } else {
        "/usr/share/sounds/timer_ring.wav"
    };
    cmd.args(["--device=master", file]);
    if let Some(ref s) = sock {
        cmd.env("PULSE_SERVER", format!("unix:{s}"));
    }
    spawn_reaped(&mut cmd);
}

fn enable_bluetooth() {
    BLUETOOTH_ACTIVE.store(true, Ordering::SeqCst);
    let _ = Command::new("hciconfig").args(["hci0", "up"]).status();
    let _ = Command::new("hciconfig").args(["hci0", "name", "Alice Capsule Mini"]).status();
    let _ = Command::new("hciconfig").args(["hci0", "sspmode", "1"]).status();
    let _ = Command::new("hciconfig").args(["hci0", "piscan"]).status();
    if let Some(s) = find_pulse_socket() {
        let env_s = format!("unix:{s}");
        let _ = Command::new("pactl").env("PULSE_SERVER", &env_s).args(["load-module", "module-bluetooth-discover"]).status();
        let _ = Command::new("pactl").env("PULSE_SERVER", &env_s).args(["load-module", "module-bluetooth-policy"]).status();
        let _ = Command::new("pactl").env("PULSE_SERVER", &env_s).args(["load-module", "module-switch-on-connect"]).status();
    }
}

fn disable_bluetooth() {
    BLUETOOTH_ACTIVE.store(false, Ordering::SeqCst);
    let _ = Command::new("hciconfig").args(["hci0", "noscan"]).status();
}

fn get_bluetooth_status() -> String {
    let out = Command::new("hciconfig").args(["hci0"]).output();
    if let Ok(o) = out {
        let text = String::from_utf8_lossy(&o.stdout);
        if text.contains("ISCAN") {
            let conn = Command::new("hcitool").args(["con"]).output();
            if let Ok(co) = conn {
                let co_text = String::from_utf8_lossy(&co.stdout);
                if co_text.contains("ACL") {
                    return "К колонке подключено устройство по блютуз.".to_string();
                }
            }
            return "Блютуз включен. Имя колонки — Alice Capsule Mini. Колонка готова к подключению.".to_string();
        }
    }
    "Блютуз выключен.".to_string()
}

fn plural_russian(n: u32, one: &str, two_four: &str, five_many: &str) -> String {
    let n100 = n % 100;
    let n10 = n % 10;
    if (11..=19).contains(&n100) {
        format!("{n} {five_many}")
    } else if n10 == 1 {
        format!("{n} {one}")
    } else if (2..=4).contains(&n10) {
        format!("{n} {two_four}")
    } else {
        format!("{n} {five_many}")
    }
}

// ---------------------------------------------------------------------------
// 8. Touch & Hardware Button Monitor Task
// ---------------------------------------------------------------------------
// Громкость — целиком прерогатива штатного voice-agent (Маруси):
// она хранит master_volume в /data/voice-agent/device_settings.json и сама
// обрабатывает кнопки Vol+/Vol-. Алиса раньше держала свой счётчик от 50% и
// перетирала настройку Маруси абсолютным значением. Здесь НЕ трогаем громкость.

async fn spawn_touch_monitor(wake_tx: mpsc::Sender<()>) {
    tokio::spawn(async move {
        println!("[Touch] Мониторинг сенсорной панели /dev/input/event0 активен.");
        let mut file = match tokio::fs::File::open(INPUT_EVENT_DEV).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[Touch] Не удалось открыть {}: {e}", INPUT_EVENT_DEV);
                return;
            }
        };

        let mut buf = [0u8; 24]; // sizeof(struct input_event) on 64-bit Linux
        while file.read_exact(&mut buf).await.is_ok() {
            let ev_type = u16::from_le_bytes([buf[16], buf[17]]);
            let ev_code = u16::from_le_bytes([buf[18], buf[19]]);
            let ev_val = i32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);

            if ev_type == 1 && (ev_val == 1 || ev_val == 2) {
                match ev_code {
                    704 | 164 => {
                        if ev_val == 1 {
                            if MIC_MUTED.load(Ordering::SeqCst) {
                                println!("\n[Touch] Кнопка действие нажата при выключенном микрофоне.");
                                set_ring("mute");
                            } else {
                                println!("\n[Touch] Кнопка действие (code={}): пробуждение Алисы!", ev_code);
                                stop_speech();
                                set_ring("listen");
                                let _ = wake_tx.try_send(());
                            }
                        }
                    }
                    248 => {
                        // Кнопка включения/выключения микрофона на верхней панели
                        if ev_val == 1 {
                            let now = Instant::now();
                            // Проверяем, не было ли свайпа регулировки громкости за последние 500мс
                            let is_vol_swipe = LAST_VOLUME_TOUCH.lock().unwrap()
                                .map(|t| now.duration_since(t) < Duration::from_millis(500))
                                .unwrap_or(false);
                            if is_vol_swipe {
                                println!("[Touch] Код 248 проигнорирован (свайп регулировки громкости).");
                                continue;
                            }

                            // Дебаунс 500мс против дребезга касания
                            let mut last_toggle = LAST_MIC_MUTE_TOGGLE.lock().unwrap();
                            if let Some(prev) = *last_toggle {
                                if now.duration_since(prev) < Duration::from_millis(500) {
                                    continue;
                                }
                            }
                            *last_toggle = Some(now);
                            drop(last_toggle);

                            let current = MIC_MUTED.load(Ordering::SeqCst);
                            let new_state = !current;
                            println!("\n[Touch] Кнопка микрофона (code 248): переключение в {}!",
                                if new_state { "MUTE (выключен)" } else { "UNMUTE (включен)" }
                            );
                            apply_mic_mute(new_state, true);
                        }
                    }
                    114 => {
                        // Volume Down
                        *LAST_VOLUME_TOUCH.lock().unwrap() = Some(Instant::now());
                        let old = CURRENT_VOLUME.load(Ordering::SeqCst);
                        let new_vol = old.saturating_sub(5);
                        CURRENT_VOLUME.store(new_vol, Ordering::SeqCst);
                        apply_volume(new_vol);
                        show_volume_overlay(new_vol);
                        set_ring_volume(new_vol);
                        println!("[Touch] Кнопка Vol-: {}%", new_vol);
                    }
                    115 => {
                        // Volume Up
                        *LAST_VOLUME_TOUCH.lock().unwrap() = Some(Instant::now());
                        let old = CURRENT_VOLUME.load(Ordering::SeqCst);
                        let new_vol = (old + 5).min(100);
                        CURRENT_VOLUME.store(new_vol, Ordering::SeqCst);
                        apply_volume(new_vol);
                        show_volume_overlay(new_vol);
                        set_ring_volume(new_vol);
                        println!("[Touch] Кнопка Vol+: {}%", new_vol);
                    }
                    _ => {}
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// 9. Main Alice Voice Daemon
// ---------------------------------------------------------------------------
#[tokio::main]
async fn main() -> Result<()> {
    println!("\n=======================================================");
    println!(">>> NATIVE RUST ALICE DAEMON FOR VK CAPSULE MINI <<<");
    println!("100% Автономная работа на устройстве (без компьютера)");
    println!("Архитектура: aarch64-unknown-linux-musl (чистый Rust)");
    println!("=======================================================\n");

    // Start background clock daemon for front 30-LED display
    spawn_clock_daemon();
    println!("[Screen] Цифровые часы запущены на передней матрице (IS31FL3236).");

    // 0. Locate PulseAudio Socket and export PULSE_SERVER
    if let Some(sock) = find_pulse_socket() {
        println!("[PulseAudio] Обнаружен сокет: {sock}");
        std::env::set_var("PULSE_SERVER", format!("unix:{sock}"));
    } else {
        eprintln!("[PulseAudio] Предупреждение: сокет в /tmp/pulse-*/native не найден сразу.");
    }
    apply_volume(CURRENT_VOLUME.load(Ordering::SeqCst));

    // 1. Silent startup: no chime
    set_ring("off");

    // 2. Load token
    let token = match find_yandex_token() {
        Some(t) => {
            println!("[Auth] Токен Яндекс найден: {}...", &t[..t.len().min(10)]);
            t
        }
        None => {
            eprintln!("[Auth] Внимание: /etc/alice/yandex_credentials.json не найден!");
            eprintln!("[Auth] Алиса переходит в режим ожидания первичной настройки через Onboarding Web Portal.");
            loop {
                sleep(Duration::from_secs(3)).await;
                if let Some(t) = find_yandex_token() {
                    break t;
                }
            }
        }
    };

    // 3. Unmute speex source and mute Marusia in PulseAudio
    let mut mic_setup = Command::new("sh");
    mic_setup.arg("-c").arg(r#"
SOCK=$(grep -o '/tmp/pulse-[^/]*/native' /proc/net/unix | head -n 1)
if [ -n "$SOCK" ]; then
    pactl --server=unix:$SOCK set-source-volume speex 98304 2>/dev/null
    pactl --server=unix:$SOCK set-source-mute speex 0 2>/dev/null
    pactl --server=unix:$SOCK load-module module-null-sink sink_name=silence 2>/dev/null
    pactl --server=unix:$SOCK set-default-source speex 2>/dev/null
    CURRENT=""
    pactl --server=unix:$SOCK list source-outputs 2>/dev/null | while IFS= read -r LINE; do
        case "$LINE" in
            "Source Output #"*) CURRENT=${LINE#Source Output #} ;;
            *'application.name ='*)
                if echo "$LINE" | grep -qi "Mailru"; then
                    [ -n "$CURRENT" ] && echo "$CURRENT"
                fi
                ;;
        esac
    done | while read -r SO; do
        pactl --server=unix:$SOCK move-source-output "$SO" silence.monitor 2>/dev/null
        pactl --server=unix:$SOCK set-source-output-mute "$SO" 1 2>/dev/null
    done
fi
"#);
    spawn_reaped(&mut mic_setup);

    // 4. Setup touch monitor & mic control
    let (wake_tx, mut wake_rx) = mpsc::channel(8);
    spawn_touch_monitor(wake_tx.clone()).await;
    spawn_mic_control_listener();
    apply_mic_mute(false, false);

    // 5. Setup Uniproxy session
    let uniproxy = UniproxySession::new(token.clone());

    // 6. Spawn PulseAudio microphone recorder with self-healing auto-respawn:
    //    parec --device=speex --rate=16000 --channels=1 --format=s16le
    println!("[Audio] Запуск захвата Athena DSP speex микрофона (16kHz 16-bit mono)...");
    let (pcm_tx, mut pcm_rx) = mpsc::channel::<Vec<u8>>(8);
    std::thread::spawn(move || {
        let mut buf = vec![0u8; CHUNK_BYTES];
        loop {
            let sock = find_pulse_socket();
            let mut parec_cmd = Command::new("parec");
            parec_cmd.args([
                "--device=speex",
                "--rate=16000",
                "--channels=1",
                "--format=s16le",
                "--latency-msec=20",
            ]);
            if let Some(ref s) = sock {
                parec_cmd.env("PULSE_SERVER", format!("unix:{s}"));
            }
            parec_cmd.stdout(Stdio::piped()).stderr(Stdio::null());

            match parec_cmd.spawn() {
                Ok(mut proc) => {
                    if let Some(mut stdout) = proc.stdout.take() {
                        while stdout.read_exact(&mut buf).is_ok() {
                            let _ = pcm_tx.try_send(buf.clone());
                        }
                    }
                    let _ = proc.kill();
                    let _ = proc.wait();
                }
                Err(e) => {
                    eprintln!("[Audio] Ошибка запуска parec: {e}");
                }
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    });

    println!("[Daemon] Голосовой сервис активен. Слушаю «Алиса» или касание панели!\n");

    // 25 чанков = 500 мс: «Алиса привет» не должен терять начало «привет».
    let mut audio_pre_buffer: VecDeque<Vec<u8>> = VecDeque::with_capacity(25);
    // Аудио до детекции wake word: «Алиса привет» должно не терять «привет».
    let mut pending_seed: Vec<Vec<u8>> = Vec::new();
    let mut noise_floor = 120.0f32;
    let mut state = "WAITING_WAKEWORD";
    let mut followup_deadline = Instant::now();
    // Защита от эха собственного ответа в первые мгновения окна продолжения.
    let mut followup_guard_until = Instant::now();
    let mut followup_speech_chunks: u32 = 0;
    // Barge-in during TTS: wake word may be masked by the speaker, so accept
    // sustained confident speech as an interrupt too.
    let mut barge_in_speech_chunks: u32 = 0;
    let mut speech_end_time = Instant::now();
    let mut last_debug_log = Instant::now();
    // Дедлайн окна ожидания команды; пока он не истёк, кольцо «listen» не гаснет.
    let mut listen_deadline = Instant::now();
    // Локальный споттер пишет в этот файл; облачный ASR в ожидании не вызывается.
    let _ = std::fs::remove_file(WAKE_TRIGGER_FILE);
    let _ = File::create(WAKE_TRIGGER_FILE);

    loop {
        // Read 100ms chunk from real-time channel
        let chunk_buf = match pcm_rx.recv().await {
            Some(b) => b,
            None => {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
        };

        // Non-blocking mic mute: does not freeze state machine
        if MIC_MUTED.load(Ordering::SeqCst) {
            noise_floor = 220.0;
        }

        // Convert chunk to i16 samples directly from speex DSP (without clipping)
        let mut samples = Vec::with_capacity(CHUNK_SAMPLES);
        for i in 0..CHUNK_SAMPLES {
            let s = i16::from_le_bytes([chunk_buf[i * 2], chunk_buf[i * 2 + 1]]);
            samples.push(s);
        }

        let rms = compute_rms(&samples);
        let peak = compute_peak(&samples);
        if rms < noise_floor {
            noise_floor = 0.90 * noise_floor + 0.10 * rms;
        } else {
            noise_floor = 0.995 * noise_floor + 0.005 * rms;
        }
        // Не даём голосу поднимать «шумовой пол» до своего уровня:
        // иначе порог речи догоняет речь и колонка перестаёт слышать.
        noise_floor = noise_floor.clamp(80.0, 220.0);

        // Feature 6: адаптивная громкость по шуму помещения (как smart_volume в Quasar).
        // Правило: не трогаем громкость, пока говорим/слушаем/звонит будильник,
        // и не перебиваем пользователя, если он сам менял громкость за последние 60 с.
        if ADAPTIVE_VOLUME.load(Ordering::SeqCst)
            && state == "WAITING_WAKEWORD"
            && !ALARM_RINGING.load(Ordering::SeqCst)
            && !MIC_MUTED.load(Ordering::SeqCst)
        {
            let due = LAST_ADAPTIVE_TICK.lock().unwrap()
                .map(|t| t.elapsed() >= Duration::from_secs(20))
                .unwrap_or(true);
            let user_touched = LAST_VOLUME_TOUCH.lock().unwrap()
                .map(|t| t.elapsed() < Duration::from_secs(60))
                .unwrap_or(false);
            if due && !user_touched {
                *LAST_ADAPTIVE_TICK.lock().unwrap() = Some(Instant::now());
                let base = ADAPTIVE_BASE_VOLUME.load(Ordering::SeqCst);
                let shift = ((noise_floor - 120.0) / 5.0) as i32;
                let target = ((base as i32) + shift).clamp(15, 45) as u32;
                let cur = CURRENT_VOLUME.load(Ordering::SeqCst);
                if target.abs_diff(cur) >= 3 {
                    let next = if target > cur { cur + 2 } else { cur - 2 };
                    CURRENT_VOLUME.store(next, Ordering::SeqCst);
                    apply_volume(next);
                    println!("[SmartVolume] фон {:.0} -> громкость {}%", noise_floor, next);
                }
            }
        }

        // Чувствительность: нормальная речь с расстояния ~1 м даёт RMS 300-800.
        // Старые пороги 280+ требовали кричать в микрофон.
        let speech_threshold = (noise_floor * 1.10 + 30.0).max(150.0);
        let silence_threshold = noise_floor * 1.05 + 30.0;

        if audio_pre_buffer.len() >= 25 {
            audio_pre_buffer.pop_front();
        }
        audio_pre_buffer.push_back(chunk_buf.clone());

        // Periodic background level logging (every 15 seconds)
        if last_debug_log.elapsed() >= Duration::from_secs(15) && state == "WAITING_WAKEWORD" {
            println!("[Audio Monitor] Фон RMS: {:.0} (Порог речи: {:.0}, Порог тишины: {:.0})", noise_floor, speech_threshold, silence_threshold);
            last_debug_log = Instant::now();
        }

        // State-driven busy sync: inform wakewordd when Alice is active.
        let waker_should_listen = !MIC_MUTED.load(Ordering::SeqCst) && (state == "WAITING_WAKEWORD" || state == "SPEAKING" || state == "FOLLOWUP_LISTEN");
        set_alice_busy(!waker_should_listen);

        // Касание панели или локальный wake word (TFLM-споттер).
        let touch_woke = wake_rx.try_recv().is_ok();
        let mut local_wake = false;
        if waker_should_listen {
            if let Ok(meta) = std::fs::metadata(WAKE_TRIGGER_FILE) {
                if meta.len() > 0 {
                    local_wake = true;
                    let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                }
            }
        } else {
            // Сбрасываем триггеры, пока Алиса записывает команду, ждёт продолжения или микрофон выключен
            let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        }
        // --- Timers & Alarms Trigger Check ---
        if !ALARM_RINGING.load(Ordering::SeqCst) {
            let mut fired = false;
            let mut kind = String::new();
            {
                let mut t_lock = LOCAL_TIMER.lock().unwrap();
                if let Some(ref t) = *t_lock {
                    if Instant::now() >= t.deadline {
                        fired = true;
                        kind = "timer".to_string();
                        *t_lock = None;
                    }
                }
            }
            if !fired {
                let mut a_lock = LOCAL_ALARM.lock().unwrap();
                if let Some(ref a) = *a_lock {
                    if Instant::now() >= a.deadline {
                        fired = true;
                        kind = "alarm".to_string();
                        *a_lock = None;
                    }
                }
            }
            if fired {
                if DND_MODE.load(Ordering::SeqCst) && kind == "alarm" {
                    println!("[Alarm] Будильник подавлен режимом «не беспокоить».");
                    continue;
                }
                println!("[Timer/Alarm] Сработал сигнал: {kind}!");
                ALARM_RINGING.store(true, Ordering::SeqCst);
                *ALARM_RINGING_KIND.lock().unwrap() = kind.clone();
                *LAST_ALARM_CHIME.lock().unwrap() = None;
                *ALARM_START_TIME.lock().unwrap() = Some(Instant::now());
                send_music_cmd("pause");
                // Используем чистый alert режим кольца (без залипания ночника)
                set_ring("alert");
            }
        }

        // If alarm/timer is ringing, play chime periodically (and auto-timeout after 60s)
        if ALARM_RINGING.load(Ordering::SeqCst) {
            let now = Instant::now();
            let timed_out = match *ALARM_START_TIME.lock().unwrap() {
                Some(start) => now >= start + Duration::from_secs(60),
                None => false,
            };
            if timed_out {
                println!("[Timer/Alarm] Авто-отключение сигнала по тайм-ауту 60 секунд.");
                ALARM_RINGING.store(false, Ordering::SeqCst);
                let _ = Command::new("killall").args(["-9", "paplay"]).status();
                set_ring("night_off");
                set_ring("off");
            } else {
                let should_play = match *LAST_ALARM_CHIME.lock().unwrap() {
                    Some(t) => now >= t + Duration::from_millis(2800),
                    None => true,
                };
                if should_play {
                    *LAST_ALARM_CHIME.lock().unwrap() = Some(now);
                    let k = ALARM_RINGING_KIND.lock().unwrap().clone();
                    play_alarm_sound(&k);
                }
            }
        }

        let is_muted = MIC_MUTED.load(Ordering::SeqCst);
        let activated = !is_muted && (touch_woke || local_wake);

        // Stop ringing on user interaction (touch or wakeword)
        if activated && ALARM_RINGING.load(Ordering::SeqCst) {
            println!("[Timer/Alarm] Остановлен пользователем.");
            ALARM_RINGING.store(false, Ordering::SeqCst);
            let _ = Command::new("killall").args(["-9", "paplay"]).status();
            set_ring("night_off");
            set_ring("off");
            if touch_woke {
                set_alice_busy(false);
                let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                state = "WAITING_WAKEWORD";
                continue;
            }
        }

        // -------------------------------------------------------------------
        // State 1: SPEAKING
        // -------------------------------------------------------------------
        if state == "SPEAKING" {
            // Watchdog: If pacat hangs or stays open past speech_end_time + 2s, force stop
            if Instant::now() >= speech_end_time + Duration::from_secs(2) && speech_still_playing() {
                println!("[Watchdog] Завершение зависшего воспроизведения речи pacat.");
                stop_speech();
            }
            let finished = Instant::now() >= speech_end_time && !speech_still_playing();
            if finished {
                if MIC_MUTED.load(Ordering::SeqCst) {
                    println!("[*] Алиса закончила ответ. Микрофон выключен (MUTE).");
                    unduck_music();
                    set_ring("mute");
                    set_alice_busy(false);
                    let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                    state = "WAITING_WAKEWORD";
                    continue;
                }
                println!("[*] Алиса закончила ответ. Слушаю продолжение 10 с...");
                unduck_music();
                set_ring("listen");
                set_alice_busy(false);
                let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                sleep(Duration::from_millis(300)).await;
                while pcm_rx.try_recv().is_ok() {}
                audio_pre_buffer.clear();
                noise_floor = 180.0;
                followup_deadline = Instant::now() + FOLLOWUP_WINDOW;
                followup_guard_until = Instant::now() + Duration::from_millis(700);
                state = "FOLLOWUP_LISTEN";
                continue;
            }

            // Прерывание (Barge-in): касание, «Алиса» или уверенная речь во время ответа.
            // Wake word может маскироваться собственным ТТС, поэтому добавляем
            // энергетический детектор с защитой от эха и ударных шумов.
            let barge_threshold = (noise_floor * 1.35 + 40.0).max(320.0);
            let crest = peak / rms.max(1.0);
            let speech_has_tail = speech_end_time > Instant::now() + Duration::from_millis(800);
            if speech_has_tail && rms > barge_threshold && crest < 8.0 {
                barge_in_speech_chunks += 1;
            } else {
                barge_in_speech_chunks = 0;
            }

            let voice_barge = barge_in_speech_chunks >= 10; // ~200 мс уверенной речи
            if touch_woke || local_wake || voice_barge {
                if touch_woke {
                    println!("\n[!] ПРЕРЫВАНИЕ КАСАНИЕМ ПАНЕЛИ.");
                } else if local_wake {
                    println!("\n[!] ПРЕРЫВАНИЕ ГОЛОСОМ: «Алиса» во время ответа.");
                } else {
                    println!("\n[!] ПРЕРЫВАНИЕ РЕЧЬЮ во время ответа (RMS={:.0} > {:.0}).", rms, barge_threshold);
                }
                stop_speech();
                play_chime();
                set_alice_busy(true);
                set_ring("listen");
                listen_deadline = Instant::now() + COMMAND_LISTEN_WINDOW;

                // Сохраняем начало фразы, которой пользователь перебил Алису.
                // Раньше эти чанки выбрасывались, и команда часто терялась.
                let mut seed: VecDeque<Vec<u8>> = audio_pre_buffer.clone();
                while let Ok(chunk) = pcm_rx.try_recv() {
                    if seed.len() >= 25 {
                        seed.pop_front();
                    }
                    seed.push_back(chunk);
                }
                pending_seed = seed.into_iter().collect();
                audio_pre_buffer.clear();
                barge_in_speech_chunks = 0;
                state = "RECORDING_COMMAND";
                continue;
            }
            continue;
        }

        // -------------------------------------------------------------------
        // State 2: FOLLOWUP_LISTEN (Слушаем продолжение диалога 4 секунды)
        // -------------------------------------------------------------------
        if state == "FOLLOWUP_LISTEN" {
            if Instant::now() >= followup_deadline {
                println!("[*] Диалог завершен: продолжения нет. Возврат в ожидание.");
                unduck_music();
                set_ring("off");
                set_alice_busy(false);
                let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                state = "WAITING_WAKEWORD";
                followup_speech_chunks = 0;
                while pcm_rx.try_recv().is_ok() {}
                audio_pre_buffer.clear();
                continue;
            }

            // Продолжение диалога: явный wake word ИЛИ уверенная речь после
            // короткой защиты от эха собственного ответа.
            if activated {
                followup_speech_chunks = 0;
                println!("[!] Активация продолжения диалога по триггеру «Алиса» / сенсору!");
                set_alice_busy(true);
                set_ring("listen");
                pending_seed = audio_pre_buffer.iter().cloned().collect();
                audio_pre_buffer.clear();
                listen_deadline = Instant::now() + COMMAND_LISTEN_WINDOW;
                state = "RECORDING_COMMAND";
                continue;
            }

            if Instant::now() < followup_guard_until {
                followup_speech_chunks = 0;
                continue;
            }

            let followup_threshold = (noise_floor * 1.2 + 50.0).max(260.0);
            let crest = peak / rms.max(1.0);
            // Клавиатура/стук дают короткий пик при низком среднем уровне.
            // Речь держит_CREST не выше ~6–7; всё, что выше, — ударный шум.
            if rms > followup_threshold && crest < 8.0 {
                followup_speech_chunks += 1;
                if followup_speech_chunks >= 6 {
                    followup_speech_chunks = 0;
                    println!("[!] Продолжение диалога: уверенная речь (RMS={:.0} > {:.0}, crest={:.1}).", rms, followup_threshold, crest);
                    set_alice_busy(true);
                    set_ring("listen");
                    pending_seed = audio_pre_buffer.iter().cloned().collect();
                    audio_pre_buffer.clear();
                    listen_deadline = Instant::now() + COMMAND_LISTEN_WINDOW;
                    state = "RECORDING_COMMAND";
                    continue;
                }
            } else {
                followup_speech_chunks = 0;
            }
        }

        // -------------------------------------------------------------------
        // State 3: WAITING_WAKEWORD
        // -------------------------------------------------------------------
        if state == "WAITING_WAKEWORD" {
            if is_muted {
                let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                continue;
            }
            if activated {
                if local_wake {
                    println!("\n[!] АКТИВАЦИЯ: wake word «Алиса».");
                } else {
                    println!("\n[!] АКТИВАЦИЯ ПО КАСАНИЮ ВЕРХНЕЙ ПАНЕЛИ!");
                    // Гасим акустический щелчок от пальца по пластику корпуса
                    sleep(Duration::from_millis(250)).await;
                    while pcm_rx.try_recv().is_ok() {}
                    audio_pre_buffer.clear();
                }
                // Мгновенный акустический сигнал активации (с гарантированным PULSE_SERVER)
                play_chime();

                set_alice_busy(true);
                let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                duck_music();
                set_ring("listen");
                // Сохраняем preacher-чанки и НЕ выбрасываем уже пришедший PCM:
                // иначе «Алиса привет» теряет вторую часть фразы.
                pending_seed = audio_pre_buffer.iter().cloned().collect();
                audio_pre_buffer.clear();
                listen_deadline = Instant::now() + COMMAND_LISTEN_WINDOW;
                state = "RECORDING_COMMAND";
                continue;
            }

            continue;
        }

        // -------------------------------------------------------------------
        // State 4: RECORDING_COMMAND (User is speaking their command)
        // -------------------------------------------------------------------
        if state == "RECORDING_COMMAND" {
            // Кольцо уже переведено в listen блоком активации/прерывания.
            // Повторная команда сбрасывала бы анимацию на первый кадр.
            let t_record_start = Instant::now();

            // Заранее устанавливаем ДВА WebSocket, пока пользователь произносит команду:
            // один для ASR, второй для Vins. Раньше Vins-канал открывался только после
            // ASR, и cold-connect съедал ~20 с — ответ приходил через 21 с после активации.
            let up_asr = uniproxy.clone();
            let warm_ws_task = tokio::spawn(async move {
                up_asr.connect_and_sync().await
            });
            let up_vins = uniproxy.clone();
            let warm_vins_ws_task = tokio::spawn(async move {
                up_vins.connect_and_sync().await
            });

            let mut command_chunks: Vec<Vec<u8>> = Vec::with_capacity(250);
            if pending_seed.len() > 25 {
                let drop_n = pending_seed.len() - 25;
                pending_seed.drain(..drop_n);
            }
            let mut pre_chunks: VecDeque<Vec<u8>> = pending_seed.drain(..).collect();

            // Wake word мог сработать уже во время фразы («Алиса привет»).
            // Если prebuffer содержит уверенную речь, не ждём НОВОГО всплеска
            // RMS — иначе «привет», сказанный сразу, теряется полностью.
            let seed_threshold = (noise_floor * 1.10 + 30.0).max(150.0);
            let seed_speech_chunks = pre_chunks
                .iter()
                .filter(|chunk| {
                    let mut sum = 0.0f64;
                    for i in 0..CHUNK_SAMPLES {
                        let s = i16::from_le_bytes([chunk[i * 2], chunk[i * 2 + 1]]) as f64;
                        sum += s * s;
                    }
                    (sum / CHUNK_SAMPLES as f64).sqrt() > seed_threshold as f64
                })
                .count();

            let mut speech_consecutive = 0;
            let mut silence_chunks = 0;
            let mut user_started_speaking = seed_speech_chunks >= 3;
            let mut heard_speech = false;
            // 10 секунд — это окно ОЖИДАНИЯ НАЧАЛА фразы. Как только человек
            // заговорил, дедлайн снимается: иначе длинная фраза обрывалась бы
            // ровно на 3-й секунде. Ограничителем дальше работает VAD по паузе.
            let mut speaking_deadline: Option<Instant> = None;

            if user_started_speaking {
                command_chunks.extend(pre_chunks.drain(..));
                listen_deadline = Instant::now() + Duration::from_secs(20);
                println!("[*] Речь уже звучала до срабатывания wake word ({} чанков) — продолжаем запись.", seed_speech_chunks);
            }

            while Instant::now() < listen_deadline {
                let remaining = listen_deadline.saturating_duration_since(Instant::now());
                let wait = remaining.min(Duration::from_millis(100));
                match tokio::time::timeout(wait, pcm_rx.recv()).await {
                    Ok(Some(next_chunk)) => {
                        let cur_samples: Vec<i16> = (0..CHUNK_SAMPLES)
                            .map(|i| i16::from_le_bytes([next_chunk[i * 2], next_chunk[i * 2 + 1]]))
                            .collect();
                        let cur_rms = compute_rms(&cur_samples);

                        let cmd_threshold = (noise_floor * 1.10 + 30.0).max(150.0);
                        if !user_started_speaking {
                            if pre_chunks.len() >= 10 {
                                pre_chunks.pop_front();
                            }
                            pre_chunks.push_back(next_chunk.clone());

                            if cur_rms > cmd_threshold {
                                speech_consecutive += 1;
                                if speech_consecutive >= 3 { // 60 мс — мягкий старт фразы
                                    user_started_speaking = true;
                                    command_chunks.extend(pre_chunks.drain(..));
                                    listen_deadline = Instant::now() + Duration::from_secs(20);
                                    println!("[*] Начало команды зафиксировано (RMS={:.0} > {:.0}).", cur_rms, cmd_threshold);
                                }
                            } else {
                                speech_consecutive = 0;
                            }
                        } else {
                            command_chunks.push(next_chunk);
                            if cur_rms > cmd_threshold {
                                silence_chunks = 0;
                            } else {
                                silence_chunks += 1;
                                // VAD: 800 мс паузы = конец фразы, минимум 300 мс речи
                                if silence_chunks >= 40 && command_chunks.len() >= 15 {
                                    println!("[*] VAD: Конец фразы зафиксирован по паузе ({} чанков, ~{:.1}с).", command_chunks.len(), (command_chunks.len() as f32) * 0.02);
                                    println!("[Latency] VAD: {} ms после активации", t_record_start.elapsed().as_millis());
                                    heard_speech = true;
                                    break;
                                }
                            }
                        }
                        if command_chunks.len() >= 400 {
                            heard_speech = true;
                            break;
                        }
                        // Потолок на длину уже начатой фразы.
                        if let Some(dl) = speaking_deadline {
                            if Instant::now() >= dl {
                                println!("[*] Достигнут предел длины фразы (15 с).");
                                heard_speech = true;
                                break;
                            }
                        }
                    }
                    _ => { /* нет чанка — продолжаем ждать до дедлайна */ }
                }
            }

            if !heard_speech {
                println!("[*] Окно ожидания команды истекло — возврат в режим ожидания.");
                unduck_music();
                set_ring("off");
                set_alice_busy(false);
                let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                state = "WAITING_WAKEWORD";
                while pcm_rx.try_recv().is_ok() {}
                audio_pre_buffer.clear();
                continue;
            }

            set_ring("think");
            println!("[*] Распознавание команды через Yandex Cloud ASR (с предварительно прогретым каналом)...");

            let pre_ws = match warm_ws_task.await {
                Ok(Ok(w)) => Some(w),
                _ => None,
            };
            // Второй канал греется параллельно с ASR, чтобы Vins не делал
            // холодный TLS-handshake после распознавания.
            let vins_ws = match warm_vins_ws_task.await {
                Ok(Ok(w)) => Some(w),
                _ => None,
            };

            let t_asr = Instant::now();
            match uniproxy.recognize_pcm_stream(&command_chunks, pre_ws).await {
                Ok(raw_text) => {
                    println!("[Latency] ASR: {} ms", t_asr.elapsed().as_millis());
                    let cmd = clean_alice_prompt(&raw_text.to_lowercase());
                    let is_noise_word = cmd == "а" || cmd == "э" || cmd == "мм" || cmd == "угу" 
                        || cmd == "ну" || cmd == "а д" || cmd == "да" || cmd == "д" || cmd == "хм";

                    // Проверка на ложные обращения к Борису / Ларисе
                    if is_boris_or_false_wake(&raw_text) && !is_alice_wake(&raw_text) {
                        println!("[Alice] Отфильтровано обращение «{}» (Борис / Лариса) — возврат в режим ожидания.", raw_text);
                        unduck_music();
                        set_ring("off");
                        set_alice_busy(false);
                        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                        state = "WAITING_WAKEWORD";
                        continue;
                    }

                    if cmd.is_empty() || (is_noise_word && !is_alice_wake(&raw_text)) {
                        if !is_noise_word && is_alice_wake(&raw_text) {
                            println!("[Alice] Обращение «Алиса» без команды. Отвечаем через Vins и ждём продолжения...");
                            speak_dynamic_reply(
                                "Привет",
                                "Слушаю вас",
                                &uniproxy,
                                vins_ws,
                                &mut speech_end_time,
                                &mut state,
                            ).await;
                            followup_deadline = Instant::now() + FOLLOWUP_WINDOW;
                            followup_guard_until = Instant::now() + Duration::from_millis(700);
                            state = "FOLLOWUP_LISTEN";
                            continue;
                        }
                        println!("[*] Отфильтрован шум / фоновый бубнёж: «{}» — возврат в ожидание.", raw_text);
                        unduck_music();
                        set_ring("off");
                        set_alice_busy(false);
                        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                        state = "WAITING_WAKEWORD";
                        continue;
                    }
                    println!("[ASR Команда]: «{}»", cmd);
                    execute_command(&cmd, &uniproxy, vins_ws, &mut speech_end_time, &mut state).await;
                }
                Err(e) => {
                    eprintln!("[ASR Error]: {e}");
                    unduck_music();
                    set_ring("off");
                    set_alice_busy(false);
                    let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
                    state = "WAITING_WAKEWORD";
                }
            }
            while let Ok(chunk) = pcm_rx.try_recv() {
                if audio_pre_buffer.len() >= 25 {
                    audio_pre_buffer.pop_front();
                }
                audio_pre_buffer.push_back(chunk);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 10. Command Classifier & Execution
// ---------------------------------------------------------------------------
/// Расстояние Левенштейна (для коротких слов, DP посимвольно).
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// Похоже ли слово на «алиса» с учётом ошибок распознавания.
fn is_wake_like(word: &str) -> bool {
    if matches!(
        word,
        "алиса" | "алису" | "алисе" | "алисой" | "алисочка" | "алисон" | "алис" | "алиска"
    ) {
        return true;
    }
    // Опечатки ASR: «алисса», «алисаа», «аллиса», «алиса́» и т.п.
    let len = word.chars().count();
    if (5..=8).contains(&len) {
        return levenshtein(word, "алиса") <= 1;
    }
    false
}

/// Проверка обращения к Алисе ПО СЛОВАМ (не подстрокой) и только в начале фразы.
/// Раньше `contains("алис")` ловило любое слово с этими буквами — колонка
/// просыпалась на всё подряд.
fn is_alice_wake(text: &str) -> bool {
    let lower = text.to_lowercase().replace('ё', "е");
    lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .take(3)
        .any(is_wake_like)
}

/// Распознавание ложных вызовов на «Борис / Бориса / Лариса»
fn is_boris_or_false_wake(text: &str) -> bool {
    let lower = text.to_lowercase().replace('ё', "е");
    let words: Vec<&str> = lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect();

    if words.is_empty() {
        return false;
    }

    let boris_patterns = [
        "борис", "бориса", "борису", "борисом", "борисе",
        "боря", "боре", "борю", "борей", "борька", "борьки", "борьке", "борьку",
        "боречка", "боренька",
        "барис", "бариса", "барису", "барисом",
        "борисыч", "барисыч", "борисов", "борисова",
        "лариса", "ларису", "ларисе", "ларисой", "лариска", "ларисочка",
        "ириса", "ириска", "барбарис", "барбариса"
    ];

    if words.len() <= 2 {
        for w in &words {
            if boris_patterns.contains(w) {
                return true;
            }
            let len = w.chars().count();
            if (4..=7).contains(&len) {
                if levenshtein(w, "борис") <= 1 || levenshtein(w, "бориса") <= 1 || levenshtein(w, "лариса") <= 1 {
                    return true;
                }
            }
        }
    }

    let first = words[0];
    if boris_patterns.contains(&first) {
        return true;
    }
    let first_len = first.chars().count();
    if (4..=7).contains(&first_len) {
        if levenshtein(first, "борис") <= 1 || levenshtein(first, "бориса") <= 1 || levenshtein(first, "лариса") <= 1 {
            return true;
        }
    }

    false
}

fn clean_alice_prompt(raw: &str) -> String {
    let lower = raw.trim().to_lowercase().replace('ё', "е");
    // Отбрасываем ВСЕ ведущие wake-слова, в том числе с опечатками ASR
    // («алисса», «аллиса»), чтобы в команду не попадал мусор.
    let words: Vec<&str> = lower.split_whitespace().collect();
    let start = words
        .iter()
        .position(|w| {
            let clean: String = w.chars().filter(|c| c.is_alphanumeric()).collect();
            !is_wake_like(&clean)
        })
        .unwrap_or(words.len());
    words[start..]
        .join(" ")
        .trim_matches(|c: char| c == ',' || c == '.' || c == '?' || c == '!' || c == ' ')
        .to_string()
}

/// Живой ответ через Yandex Vins. Локальная фраза используется только если
/// интернет недоступен — обычные реплики больше не берутся из захардкоженного списка.
async fn speak_dynamic_reply(
    prompt: &str,
    fallback: &str,
    uniproxy: &UniproxySession,
    vins_ws: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    speech_end_time: &mut Instant,
    state: &mut &'static str,
) {
    set_alice_busy(true);
    set_ring("think");
    let stream_start = Instant::now();
    let reply_tx = spawn_speech_stream(SAMPLE_RATE_PLAY);
    match uniproxy.ask_vins_and_stream(prompt, reply_tx, vins_ws).await {
        Ok((reply, dur, has_music, has_pause, _track_id)) => {
            println!("[Dynamic] Vins: «{reply}» (запрос: «{prompt}»)");
            set_ring("speak");
            if has_pause {
                send_music_cmd("pause");
            }
            if has_music {
                send_music_cmd("wave");
                set_ring("music");
            }
            let remaining = dur.saturating_sub(stream_start.elapsed());
            *speech_end_time = Instant::now() + remaining + Duration::from_millis(150);
            *state = "SPEAKING";
        }
        Err(e) => {
            eprintln!("[Dynamic] Vins недоступен ({e}); локальный ответ «{fallback}».");
            speak_text(fallback, uniproxy, speech_end_time, state).await;
        }
    }
}

async fn execute_command(
    cmd: &str,
    uniproxy: &UniproxySession,
    vins_ws: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    speech_end_time: &mut Instant,
    state: &mut &'static str,
) {
    // 0. Stop ringing alarm or timer
    if ALARM_RINGING.load(Ordering::SeqCst) {
        println!("[Timer/Alarm] Остановка по голосовой команде: «{cmd}».");
        ALARM_RINGING.store(false, Ordering::SeqCst);
        let _ = Command::new("killall").args(["-9", "paplay"]).status();
        set_ring("night_off");
        set_ring("off");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 0.05 Feature 5: reminders («напомни мне через N минут …»)
    if let Some((secs, what)) = parse_reminder_command(cmd) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        *LOCAL_TIMER.lock().unwrap() = Some(ActiveTimer {
            label: format!("напомнить:{what}"),
            total_secs: secs,
            deadline,
            announce: Some(what.clone()),
        });
        println!("[Reminder] Напоминание через {secs} сек: «{what}»");
        speak_text(&format!("Напомню через {secs} секунд: {what}."), uniproxy, speech_end_time, state).await;
        return;
    }

    // 0.1 Local Timers
    if let Some(action) = parse_timer_command(cmd) {
        match action {
            TimerAction::Set(secs, label) => {
                let deadline = Instant::now() + Duration::from_secs(secs);
                *LOCAL_TIMER.lock().unwrap() = Some(ActiveTimer {
                    label: label.clone(),
                    total_secs: secs,
                    deadline,
                    announce: if label.starts_with("напомнить:") {
                        Some(label.trim_start_matches("напомнить:").to_string())
                    } else {
                        None
                    },
                });
                println!("[Timer] Установлен таймер на {label} ({secs} сек)");
                speak_text(&format!("Установила таймер на {label}."), uniproxy, speech_end_time, state).await;
                return;
            }
            TimerAction::Status => {
                let rem_opt = LOCAL_TIMER.lock().unwrap().as_ref().map(|t| {
                    let now = Instant::now();
                    if t.deadline > now {
                        t.deadline.duration_since(now).as_secs()
                    } else {
                        0
                    }
                });
                if let Some(rem) = rem_opt {
                    let m = (rem / 60) as u32;
                    let s = (rem % 60) as u32;
                    let m_str = plural_russian(m, "минута", "минуты", "минут");
                    let s_str = plural_russian(s, "секунда", "секунды", "секунд");
                    let text = if m > 0 && s > 0 {
                        format!("Осталось {m_str} и {s_str}.")
                    } else if m > 0 {
                        format!("Осталось {m_str}.")
                    } else {
                        format!("Осталось {s_str}.")
                    };
                    speak_text(&text, uniproxy, speech_end_time, state).await;
                } else {
                    speak_text("Активных таймеров нет.", uniproxy, speech_end_time, state).await;
                }
                return;
            }
            TimerAction::Cancel => {
                *LOCAL_TIMER.lock().unwrap() = None;
                println!("[Timer] Таймер отменён.");
                speak_text("Таймер отменён.", uniproxy, speech_end_time, state).await;
                return;
            }
        }
    }

    // 0.2 Local Alarms
    if let Some(action) = parse_alarm_command(cmd) {
        match action {
            AlarmAction::Set(h, m, time_str) => {
                let diff = calculate_alarm_diff_secs(h, m);
                let deadline = Instant::now() + Duration::from_secs(diff);
                *LOCAL_ALARM.lock().unwrap() = Some(ActiveAlarm {
                    time_str: time_str.clone(),
                    target_hour: h,
                    target_minute: m,
                    deadline,
                });
                println!("[Alarm] Установлен будильник на {time_str} (через {diff} сек)");
                speak_text(&format!("Будильник установлен на {time_str}."), uniproxy, speech_end_time, state).await;
                return;
            }
            AlarmAction::Status => {
                let rem_opt = LOCAL_ALARM.lock().unwrap().as_ref().map(|a| {
                    let now = Instant::now();
                    let diff = if a.deadline > now {
                        a.deadline.duration_since(now).as_secs()
                    } else {
                        0
                    };
                    (a.time_str.clone(), diff)
                });
                if let Some((t_str, diff)) = rem_opt {
                    let h = (diff / 3600) as u32;
                    let m = ((diff % 3600) / 60) as u32;
                    let h_str = plural_russian(h, "час", "часа", "часов");
                    let m_str = plural_russian(m, "минута", "минуты", "минут");
                    let text = if h > 0 {
                        format!("Будильник на {t_str} сработает через {h_str} и {m_str}.")
                    } else {
                        format!("Будильник на {t_str} сработает через {m_str}.")
                    };
                    speak_text(&text, uniproxy, speech_end_time, state).await;
                } else {
                    speak_text("Активных будильников нет.", uniproxy, speech_end_time, state).await;
                }
                return;
            }
            AlarmAction::Cancel => {
                *LOCAL_ALARM.lock().unwrap() = None;
                println!("[Alarm] Будильник отменён.");
                speak_text("Будильник отменён.", uniproxy, speech_end_time, state).await;
                return;
            }
        }
    }

    // 0.3 Bluetooth Speaker Controls
    if cmd.contains("блютуз") || cmd.contains("bluetooth") {
        if cmd.contains("выключи") || cmd.contains("отключи") || cmd.contains("выруби") {
            disable_bluetooth();
            println!("[BT] Блютуз выключен.");
            speak_text("Блютуз выключен.", uniproxy, speech_end_time, state).await;
            return;
        } else if cmd.contains("статус") || cmd.contains("состояние") {
            let st = get_bluetooth_status();
            speak_text(&st, uniproxy, speech_end_time, state).await;
            return;
        } else if cmd.contains("включи") || cmd.contains("подключи") || cmd.contains("режим") || cmd == "блютуз" || cmd == "bluetooth" {
            enable_bluetooth();
            println!("[BT] Блютуз включен (Alice Capsule Mini).");
            speak_text("Блютуз включен. Имя колонки — Alice Capsule Mini. Теперь вы можете подключиться с телефона.", uniproxy, speech_end_time, state).await;
            return;
        }
    }

    // 0.4 Whisper Mode Controls
    if cmd.contains("шёпот") || cmd.contains("шепот") || cmd.contains("шепчи") {
        if cmd.contains("выключи") || cmd.contains("хватит") || cmd.contains("обычн") || cmd.contains("нормальн") || cmd.contains("отключи") {
            WHISPER_MODE.store(false, Ordering::SeqCst);
            println!("[Whisper] Режим шёпота выключен.");
            speak_text("Договорились, говорю обычным голосом.", uniproxy, speech_end_time, state).await;
            return;
        } else {
            WHISPER_MODE.store(true, Ordering::SeqCst);
            println!("[Whisper] Режим шёпота включен.");
            speak_text("Хорошо, теперь я буду шептать.", uniproxy, speech_end_time, state).await;
            return;
        }
    }
    if cmd.contains("обычный голос") || cmd.contains("говори нормально") {
        WHISPER_MODE.store(false, Ordering::SeqCst);
        println!("[Whisper] Режим шёпота выключен.");
        speak_text("Договорились, говорю обычным голосом.", uniproxy, speech_end_time, state).await;
        return;
    }

    // 0.4.1 Приветствия и small talk идут через Vins: ответ Live/интернет,
    // локальная фраза остаётся только как fallback без сети.
    let is_greeting = matches!(
        cmd,
        "привет" | "приветик" | "здравствуй" | "здравствуйте" | "хай"
            | "добрый день" | "добрый вечер" | "доброе утро"
            | "как дела" | "как ты" | "спасибо" | "благодарю"
    );
    if is_greeting {
        // Vins ждёт естественную реплику пользователя. Инструкции в text
        // («Ответь коротко…») он не понимает и возвращает служебный отказ.
        speak_dynamic_reply(cmd, "Привет!", uniproxy, vins_ws, speech_end_time, state).await;
        return;
    }

    // 0.5 Voice Microphone Controls ("выключи микрофон", "отключи микрофон", "выруби микрофон")
    if cmd.contains("выключи микрофон") || cmd.contains("отключи микрофон")
        || cmd.contains("заглуши микрофон") || cmd.contains("выруби микрофон")
        || cmd.contains("выключить микрофон") || cmd.contains("отключить микрофон")
        || cmd.contains("поставь микрофон на паузу") || cmd.contains("не слушай")
        || cmd.contains("выключи свой микрофон")
    {
        println!("[Voice Command] Выключение микрофона по голосовой команде.");
        speak_text("Выключаю микрофон. Чтобы включить его снова, нажмите кнопку микрофона на колонке.", uniproxy, speech_end_time, state).await;
        apply_mic_mute(true, false);
        return;
    }

    if cmd.contains("статус микрофона") || cmd.contains("микрофон включен") || cmd.contains("микрофон работает") {
        speak_text("Микрофон включен и работает в штатном режиме.", uniproxy, speech_end_time, state).await;
        return;
    }

    // 1. Voice Mute ("молчи", "замолчи", "тихо", "стоп", "хватит", "отмена")
    if cmd == "молчи" || cmd == "замолчи" || cmd == "тихо" || cmd == "стоп" || cmd == "отмена" || cmd == "хватит" {
        println!("[Audio] Команда остановки речи и звука.");
        stop_speech();
        send_music_cmd("pause");
        unduck_music();
        set_ring("off");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 2. Music Stop / Pause
    if cmd.contains("останови") || cmd.contains("пауз") || cmd.contains("выключи музык")
        || cmd.contains("выключи трек") || cmd.contains("выключи песн")
        || cmd.contains("стоп музык") || cmd.contains("останови трек")
        || cmd.contains("останови песн") || cmd.contains("хватит музык")
        || cmd.contains("заглуши") || cmd.contains("выруби") {
        println!("[Music] Остановка музыки.");
        stop_speech();
        send_music_cmd("pause");
        unduck_music();
        set_ring("off");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 3. Fast Local Time (0 latency)
    if cmd.contains("сколько времени") || cmd.contains("который час") || cmd.contains("скажи время") {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let total_secs = now.as_secs() + 3 * 3600; // MSK +3
        let hours = ((total_secs % 86400) / 3600) as u32;
        let mins = ((total_secs % 3600) / 60) as u32;
        let h_str = plural_russian(hours, "час", "часа", "часов");
        let m_str = plural_russian(mins, "минута", "минуты", "минут");
        let text = if mins > 0 {
            format!("Сейчас {h_str} {m_str}.")
        } else {
            format!("Сейчас ровно {h_str}.")
        };
        speak_text(&text, uniproxy, speech_end_time, state).await;
        return;
    }

    // 4. Volume Voice Controls
    if cmd.contains("громче") || cmd.contains("прибавь") {
        let old = CURRENT_VOLUME.load(Ordering::SeqCst);
        let new_vol = (old + 10).min(100);
        CURRENT_VOLUME.store(new_vol, Ordering::SeqCst);
        apply_volume(new_vol);
        show_volume_overlay(new_vol);
        set_ring_volume(new_vol);
        speak_text(&format!("Громкость {new_vol} процентов."), uniproxy, speech_end_time, state).await;
        return;
    }

    if cmd.contains("тише") || cmd.contains("убавь") {
        let old = CURRENT_VOLUME.load(Ordering::SeqCst);
        let new_vol = old.saturating_sub(10);
        CURRENT_VOLUME.store(new_vol, Ordering::SeqCst);
        apply_volume(new_vol);
        show_volume_overlay(new_vol);
        set_ring_volume(new_vol);
        speak_text(&format!("Громкость {new_vol} процентов."), uniproxy, speech_end_time, state).await;
        return;
    }

    if cmd.contains("громкость") {
        let mut target = None;
        for token in cmd.split_whitespace() {
            if let Ok(v) = token.parse::<u32>() {
                target = Some(v.min(100));
                break;
            }
            match token {
                "ноль" => { target = Some(0); break; }
                "десять" => { target = Some(10); break; }
                "двадцать" => { target = Some(20); break; }
                "тридцать" => { target = Some(30); break; }
                "сорок" => { target = Some(40); break; }
                "пятьдесят" => { target = Some(50); break; }
                "шестьдесят" => { target = Some(60); break; }
                "семьдесят" => { target = Some(70); break; }
                "восемьдесят" => { target = Some(80); break; }
                "девяносто" => { target = Some(90); break; }
                "сто" | "максимум" => { target = Some(100); break; }
                _ => {}
            }
        }
        if let Some(v) = target {
            CURRENT_VOLUME.store(v, Ordering::SeqCst);
            apply_volume(v);
            show_volume_overlay(v);
            set_ring_volume(v);
            speak_text(&format!("Установила громкость на {v} процентов."), uniproxy, speech_end_time, state).await;
            return;
        }
    }

    // 4.5 Feature 5: «не беспокоить» — глушим напоминания и будильники
    if cmd.contains("не беспокоить") || cmd.contains("днд") {
        let turn_off = cmd.contains("выключ") || cmd.contains("отключ") || cmd.contains("сними") || cmd.contains("отмени");
        DND_MODE.store(!turn_off, Ordering::SeqCst);
        let reply = if turn_off { "Режим «не беспокоить» выключен." } else { "Включаю режим «не беспокоить»." };
        speak_text(reply, uniproxy, speech_end_time, state).await;
        return;
    }

    // 4.6 Feature 6: адаптивная громкость
    if cmd.contains("адаптивн") || cmd.contains("автогромкост") || cmd.contains("громкость по шуму") {
        let turn_off = cmd.contains("выключ") || cmd.contains("отключ") || cmd.contains("перестань");
        ADAPTIVE_VOLUME.store(!turn_off, Ordering::SeqCst);
        if !turn_off {
            ADAPTIVE_BASE_VOLUME.store(CURRENT_VOLUME.load(Ordering::SeqCst), Ordering::SeqCst);
        }
        let reply = if turn_off { "Адаптивная громкость выключена." } else { "Адаптивная громкость включена." };
        speak_text(reply, uniproxy, speech_end_time, state).await;
        return;
    }

    // 4.7 Радио: «Моя волна» или поиск станции через Яндекс Музыку
    if cmd.contains("радио") || cmd.contains("мою волну") || cmd.contains("моя волна") || cmd.contains("волну") {
        let stop = cmd.contains("выключ") || cmd.contains("останови") || cmd.contains("хватит");
        if stop {
            send_music_cmd("stop");
            speak_text("Радио выключено.", uniproxy, speech_end_time, state).await;
        } else if cmd.contains("мою волну") || cmd.contains("моя волна") || !cmd.contains("радио") {
            send_music_cmd("wave");
            speak_text("Включаю мою волну.", uniproxy, speech_end_time, state).await;
        } else {
            let query = cmd
                .split_once("радио")
                .map(|(_, rest)| rest.trim().to_string())
                .unwrap_or_default();
            if query.is_empty() {
                send_music_cmd("wave");
                speak_text("Включаю мою волну.", uniproxy, speech_end_time, state).await;
            } else if play_named_radio(&query).await {
                speak_text(&format!("Включаю радио: {query}."), uniproxy, speech_end_time, state).await;
            } else {
                speak_text(&format!("Не нашла радиостанцию «{query}»."), uniproxy, speech_end_time, state).await;
            }
        }
        return;
    }

    // 5. LED Ring Nightlight Controls
    if cmd.contains("ночник") || cmd.contains("подсветк") || cmd.contains("светильник") {
        if cmd.contains("выключи") || cmd.contains("выруби") || cmd.contains("погаси") || cmd.contains("отключи") {
            set_ring("night_off");
            speak_text("Выключила ночник.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("красн") {
            set_ring("night_color 255 10 10 0.8");
            speak_text("Включила красный ночник.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("зелен") {
            set_ring("night_color 10 255 30 0.8");
            speak_text("Включила зеленый ночник.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("син") || cmd.contains("голуб") {
            set_ring("night_color 0 80 255 0.8");
            speak_text("Включила синий ночник.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("желт") || cmd.contains("тепл") {
            set_ring("night_color 255 160 20 0.8");
            speak_text("Включила теплый желтый свет.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("фиолетов") || cmd.contains("сирен") {
            set_ring("night_color 160 20 255 0.8");
            speak_text("Включила фиолетовый ночник.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("бел") {
            set_ring("night_color 255 255 255 0.8");
            speak_text("Включила белый свет.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("градиент") || cmd.contains("радуг") || cmd.contains("перелив") {
            set_ring("night_mode gradient");
            speak_text("Включила переливающийся ночник.", uniproxy, speech_end_time, state).await;
            return;
        }
        if cmd.contains("дыхан") || cmd.contains("пульс") {
            set_ring("night_mode breathing");
            speak_text("Включила режим мягкого дыхания.", uniproxy, speech_end_time, state).await;
            return;
        }
        set_ring("night_on");
        speak_text("Включила ночник.", uniproxy, speech_end_time, state).await;
        return;
    }

    // 6. Next Track (перелистывание / пропуск)
    if cmd.contains("следующ") || cmd.contains("дальше") || cmd.contains("переключ")
        || cmd.contains("перелист") || cmd.contains("листай") || cmd.contains("вперед")
        || cmd.contains("пропусти") || cmd.contains("другой трек") || cmd.contains("другую песн") {
        println!("[Music] Следующий трек");
        send_music_cmd("next");
        set_ring("music");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 7. Previous Track
    if cmd.contains("предыдущ") || cmd.contains("назад") || cmd.contains("верни")
        || cmd.contains("прошлый трек") || cmd.contains("прошлую песн") {
        println!("[Music] Предыдущий трек");
        send_music_cmd("prev");
        set_ring("music");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 8. Resume / Continue Music
    if cmd.contains("продолжи") || cmd.contains("продолжай") || cmd.contains("возобнови")
        || cmd.contains("играй") || cmd.contains("сними с пауз") {
        println!("[Music] Возобновление музыки");
        send_music_cmd("resume");
        set_ring("music");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 9. What is playing
    if cmd.contains("что играет") || cmd.contains("какая песня") || cmd.contains("какой трек")
        || cmd.contains("кто поет") || cmd.contains("что за песня") || cmd.contains("что за трек")
        || cmd.contains("название трека") || cmd.contains("название песни") {
        send_music_cmd("what_is_playing");
        set_alice_busy(false);
        let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
        *state = "WAITING_WAKEWORD";
        return;
    }

    // 10. Like & Dislike
    if cmd.contains("лайк") || cmd.contains("мне нравится") || cmd.contains("классный трек")
        || cmd.contains("добавь в любим") || cmd.contains("добавь в избран")
        || cmd.contains("сохрани песн") || cmd.contains("сохрани трек") {
        println!("[Music] Лайк");
        send_music_cmd("like");
        speak_text("Добавила в любимые!", uniproxy, speech_end_time, state).await;
        return;
    }

    if cmd.contains("дизлайк") || cmd.contains("не нравится") || cmd.contains("больше не ставь")
        || cmd.contains("удали из любим") {
        println!("[Music] Дизлайк");
        send_music_cmd("dislike");
        speak_text("Поняла, больше не буду это ставить.", uniproxy, speech_end_time, state).await;
        return;
    }

    // 11. My Wave & Liked Playlist
    if cmd.contains("волн") || cmd == "включи музыку" || cmd == "поставь музыку"
        || cmd == "сыграй музыку" || cmd == "музыка" || cmd == "включи мою"
        || cmd == "включи мое" || cmd == "включи что-нибудь" || cmd == "поставь что-нибудь"
        || cmd == "мою" || cmd == "мое" {
        println!("[Music] Включение «Моей Волны»");
        send_music_cmd("wave");
        set_ring("music");
        speak_text("Включаю мою волну!", uniproxy, speech_end_time, state).await;
        return;
    }

    if (cmd.contains("любим") && (cmd.contains("песн") || cmd.contains("трек") || cmd.contains("музык")))
        || cmd.contains("мои треки") || cmd.contains("моя музыка") || cmd.contains("избранное") {
        println!("[Music] Включение любимых треков");
        send_music_cmd("liked");
        set_ring("music");
        speak_text("Включаю ваши любимые треки!", uniproxy, speech_end_time, state).await;
        return;
    }

    // 12. Sleep Timer
    if cmd.contains("таймер сна") || cmd.contains("выключи через") || cmd.contains("засни через") {
        let mut mins = 30u64;
        for token in cmd.split_whitespace() {
            if let Ok(m) = token.parse::<u64>() {
                mins = m;
                break;
            }
        }
        send_music_cmd(&format!("sleep_timer:{}", mins * 60));
        speak_text(&format!("Установила таймер сна на {mins} минут."), uniproxy, speech_end_time, state).await;
        return;
    }

    // 13. Direct Music Track / Artist / Album / Playlist Search
    let search_prefixes = [
        "включи песню ", "поставь песню ", "включи трек ", "поставь трек ",
        "найди песню ", "найди трек ", "найди ",
        "включи исполнителя ", "поставь исполнителя ", "включи группу ", "поставь группу ",
        "включи альбом ", "поставь альбом ", "включи плейлист ", "поставь плейлист ",
        "послушаем ", "запусти ", "сыграй ", "включи ", "поставь "
    ];
    for pfx in search_prefixes {
        if let Some(query) = cmd.strip_prefix(pfx) {
            let clean_query = query.trim();
            let is_system = clean_query.starts_with("ночник")
                || clean_query.starts_with("свет")
                || clean_query.starts_with("подсветк")
                || clean_query.starts_with("громкост")
                || clean_query.starts_with("звук")
                || clean_query.starts_with("таймер")
                || clean_query.starts_with("будильник")
                || clean_query.starts_with("час")
                || clean_query.starts_with("время");
            if clean_query == "мою" || clean_query == "мое" || clean_query == "музыку" || clean_query == "что-нибудь" || clean_query == "песню" || clean_query == "трек" {
                println!("[Music] Включение «Моей Волны» по короткой команде: «{clean_query}»");
                send_music_cmd("wave");
                set_ring("music");
                speak_text("Включаю мою волну!", uniproxy, speech_end_time, state).await;
                return;
            }
            if !clean_query.is_empty() && !is_system {
                println!("[Music] Прямой поиск трека/исполнителя: «{clean_query}»");
                send_music_cmd(&format!("track:{clean_query}"));
                set_ring("music");
                speak_text(&format!("Включаю {clean_query}."), uniproxy, speech_end_time, state).await;
                return;
            }
        }
    }

    // 7. General Conversational Query via Uniproxy Vins
    println!("[Vins] Отправка запроса в Yandex Vins: «{}»...", cmd);
    // Засекаем старт: pacat начинает играть первый чанк почти сразу, поэтому
    // остаток реплики считаем от этого момента, а не от конца синтеза.
    let stream_start = Instant::now();
    let reply_tx = spawn_speech_stream(SAMPLE_RATE_PLAY);

    set_ring("think");
    match uniproxy.ask_vins_and_stream(cmd, reply_tx, vins_ws).await {
        Ok((reply, dur, has_music, has_pause, track_id)) => {
            let stream_elapsed = stream_start.elapsed();
            println!("[Vins Ответ]: «{}» (PCM {:.2} с, уже проиграно {:.2} с)",
                     reply, dur.as_secs_f32(), stream_elapsed.as_secs_f32());
            set_ring("speak");
            if has_pause {
                send_music_cmd("pause");
            }
            if has_music {
                if let Some(tid) = track_id {
                    send_music_cmd(&format!("play:{tid}"));
                } else {
                    send_music_cmd("wave");
                }
                set_ring("music");
            }
            // ВАЖНО: `dur` — полная длина PCM, но часть её уже отыграна, пока
            // мы ждали последний чанк синтеза. Раньше сюда подставляли `now + dur`,
            // и Алиса «глохла» ещё на всю длину реплики: пользователь говорил
            // в пустоту. Теперь ждём только остаток.
            let remaining = dur.saturating_sub(stream_elapsed);
            *speech_end_time = Instant::now() + remaining + Duration::from_millis(150);
            *state = "SPEAKING";
        }
        Err(e) => {
            eprintln!("[Vins Error]: {e}");
            speak_text("Извините, не удалось связаться с сервером.", uniproxy, speech_end_time, state).await;
        }
    }
}

async fn speak_text(
    text: &str,
    uniproxy: &UniproxySession,
    speech_end_time: &mut Instant,
    state: &mut &'static str,
) {
    speak_text_with_ws(text, uniproxy, None, speech_end_time, state).await;
}

/// Как speak_text, но с уже прогретым WebSocket: не делает холодный
/// TLS-handshake между «Алиса» и ответом «Слушаю вас».
async fn speak_text_with_ws(
    text: &str,
    uniproxy: &UniproxySession,
    tts_ws: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    speech_end_time: &mut Instant,
    state: &mut &'static str,
) {
    set_alice_busy(true);
    // Пока TTS синтезируется — кольцо думает. «speak» включаем только
    // перед реальным воспроизведением PCM.
    set_ring("think");
    match uniproxy.synthesize_tts_pcm(text, tts_ws).await {
        Ok(pcm) => {
            set_ring("speak");
            let dur = play_speech_pcm(&pcm, SAMPLE_RATE_PLAY);
            *speech_end_time = Instant::now() + dur + Duration::from_millis(150);
            *state = "SPEAKING";
        }
        Err(e) => {
            eprintln!("[TTS Error]: {e}");
            set_ring("off");
            set_alice_busy(false);
            let _ = std::fs::write(WAKE_TRIGGER_FILE, "");
            *state = "WAITING_WAKEWORD";
        }
    }
}