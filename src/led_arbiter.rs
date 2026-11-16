//! Единственный Hub-owned writer для LED-кольца и экрана.
//!
//! Ring: /dev/topleds, WS2812B, 36 каналов, порядок G/R/B.
//! Screen: /sys/class/leds/clock-led1..30/brightness.
//!
//! Цвет ночника по умолчанию читается из:
//! /data/voice-agent/device_settings.json -> night_light_mode.color
//!
//! Поддерживает прямое управление через веб-панель / API:
//! - кольцо: вкл/выкл ночника, выбор цвета (палитра или HEX #RRGGBB),
//!   режимы (static, breathing, gradient, off), яркость, скорость анимации,
//!   сброс на авто-режим Маруси;
//! - экран: выбор режима (clock - обычные часы, off - выключен, eyes - глазки,
//!   solid - свечение), регулировка яркости (0..255).

use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    os::fd::AsRawFd,
    sync::{Arc, Condvar, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const RING_DEVICE: &str = "/dev/topleds";
const SCREEN_DEVICE: &str = "/dev/is31fl32xx";
const SETTINGS_PATH: &str = "/data/voice-agent/device_settings.json";

const RING_CHANNELS: usize = 36;
const SCREEN_CHANNELS: usize = 30;

const RING_WRITE: libc::c_int = 0x4010_7a01;
const SCREEN_WRITE: libc::c_int = 0xc010_6b01_u32 as libc::c_int;
const FLOOD_BATCH: usize = 256;
const MAX_BLANK: Duration = Duration::from_secs(8);

#[repr(C)]
struct ScreenFrameReq {
    start: libc::c_uint,
    count: libc::c_uint,
    data: *mut u8,
}

const SETTINGS_POLL_INTERVAL: Duration = Duration::from_millis(150);
const SETTINGS_APPLY_GRACE: Duration = Duration::from_millis(1200);
const IDLE_WAIT: Duration = Duration::from_millis(100);
const SCREEN_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// 0-индексированные светодиоды экрана (0..29 -> clock-led1..30).
/// Физическая раскладка индикатора IS31FL3236 на плате VK Капсулы Мини:
/// D1 (левый глаз): a=1, b=28, c=21, d=17, e=22, f=29, g=24 (clock-led 2, 29, 22, 18, 23, 30, 25)
/// D4 (правый глаз): a=10, b=12, c=11, d=14, e=13, f=8, g=9 (clock-led 11, 13, 12, 15, 14, 9, 10)
/// Colon (нос/точка): clock-led16 -> index 15
/// Полноразмерные высокие глазки 0 . 0 (все внешние сегменты a,b,c,d,e,f):
const ZERO_OPEN: &[usize] = &[1, 8, 10, 11, 12, 13, 14, 15, 17, 21, 22, 28, 29];
const BLINK_CLOSED: &[usize] = &[9, 15, 24];
const LOOK_LEFT: &[usize] = &[1, 8, 9, 10, 13, 14, 15, 17, 22, 24, 29];
const LOOK_RIGHT: &[usize] = &[1, 9, 10, 11, 12, 14, 15, 17, 21, 24, 28];
const WINK_LEFT: &[usize] = &[1, 9, 15, 17, 21, 22, 28, 29]; // левый 0, правый _
const WINK_RIGHT: &[usize] = &[8, 10, 11, 12, 13, 14, 15, 24]; // левый _, правый 0
const SQUINT_HAPPY: &[usize] = &[1, 8, 10, 12, 15, 28, 29]; // весёлый прищур ^ . ^
/// Ночные спящие глазки u . u: нижние полукруги D1(22, 17, 21) + точка (15) + D4(13, 14, 11).
const SLEEPING_SEGMENTS: &[usize] = &[11, 13, 14, 15, 17, 21, 22];

pub fn get_local_hour() -> u32 {
    unsafe {
        let mut t: libc::time_t = 0;
        libc::time(&mut t);
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&t, &mut tm);
        tm.tm_hour as u32
    }
}

pub fn is_night_time() -> bool {
    let hour = get_local_hour();
    hour >= 23 || hour < 7
}

/// Аварийный фиолетовый fallback.
/// Порядок каналов драйвера G, R, B.
/// 12 светодиодов × [G=25, R=85, B=130].
const NIGHT_FALLBACK: [u8; RING_CHANNELS] = {
    let mut buf = [0u8; RING_CHANNELS];
    let mut i = 0;

    while i < RING_CHANNELS {
        buf[i] = 25;
        buf[i + 1] = 85;
        buf[i + 2] = 130;
        i += 3;
    }

    buf
};

#[repr(C)]
struct FrameReq {
    start: libc::c_uint,
    count: libc::c_uint,
    data: *mut u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NightSettings {
    color: String,
    mode: String,
    speed: u8,
    revision: Option<u64>,
    frame: [u8; RING_CHANNELS],
    known_color: bool,
}

#[derive(Default)]
struct State {
    generation: u64,

    night_light: bool,
    night_frame: Option<[u8; RING_CHANNELS]>,

    night_mode: String,
    night_speed: u8,
    night_brightness: u8,
    settings: Option<NightSettings>,

    user_ring_override: bool,
    user_color_name: String,
    user_base_rgb: (u8, u8, u8),

    /// После изменения color/mode static даём voice-agent время
    /// самостоятельно применить настройку и нарисовать новый кадр.
    color_grace_until: Option<Instant>,

    /// Пока deadline не истёк, ночник не перехватывает кольцо.
    interacting_until: Option<Instant>,

    blank_until: Option<Instant>,

    /// Однократный кадр, отправляемый независимо от flood.
    /// Используется для строгих нулей при выключении ночника.
    pending_frame: Option<[u8; RING_CHANNELS]>,

    screen_mode: String,
    screen_brightness: u8,
    screen_frame: Option<[u8; SCREEN_CHANNELS]>,

    auto_brightness: bool,
    ambient_lux: f32,
    active_emotion: Option<(String, Instant, Instant)>,
    emotion_frames: Option<Vec<(f64, [u8; SCREEN_CHANNELS])>>,

    music_active: bool,

    stopping: bool,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

pub struct LedArbiter {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

static GLOBAL: OnceLock<LedArbiter> = OnceLock::new();

fn bump(state: &mut State) {
    state.generation = state.generation.wrapping_add(1);
}

fn ring_frame(state: &State) -> [u8; RING_CHANNELS] {
    if state.night_light {
        state.night_frame.unwrap_or(NIGHT_FALLBACK)
    } else {
        [0; RING_CHANNELS]
    }
}

fn interacting_active(state: &State, now: Instant) -> bool {
    state
        .interacting_until
        .map(|until| now < until)
        .unwrap_or(false)
}

fn blank_active(state: &State, now: Instant) -> bool {
    state
        .blank_until
        .map(|until| now < until)
        .unwrap_or(false)
}

fn color_grace_active(state: &State, now: Instant) -> bool {
    state
        .color_grace_until
        .map(|until| now < until)
        .unwrap_or(false)
}

fn flood_active(state: &State, now: Instant) -> bool {
    if blank_active(state, now) {
        return true;
    }

    if state.user_ring_override {
        return state.night_light
            && !interacting_active(state, now)
            && state.night_mode != "off";
    }

    state.night_light
        && !interacting_active(state, now)
        && !color_grace_active(state, now)
        && !state.night_mode.eq_ignore_ascii_case("breathing")
}

pub fn color_rgb(name: &str) -> Option<(u8, u8, u8)> {
    match name.trim().to_ascii_lowercase().as_str() {
        "red" | "красный" => Some((0xFF, 0x00, 0x00)),
        "yellow" | "жёлтый" | "желтый" => Some((0xB0, 0x50, 0x00)),
        "green" | "зелёный" | "зеленый" => Some((0x00, 0xFF, 0x00)),
        "cyan" | "бирюзовый" | "голубой" => Some((0x00, 0xBF, 0xFF)),
        "blue" | "синий" => Some((0x00, 0x00, 0xFF)),
        "purple" | "фиолетовый" | "сиреневый" => Some((0x80, 0x00, 0x80)),
        "turquoise" => Some((0x00, 0x73, 0x2F)),
        "orange" | "оранжевый" => Some((0xFF, 0x4B, 0x00)),
        "white" | "белый" => Some((0xFF, 0xFF, 0xFF)),
        "pink" | "розовый" => Some((0xFF, 0x14, 0x93)),
        _ => None,
    }
}

pub fn parse_color(name_or_hex: &str) -> Option<((u8, u8, u8), String, String)> {
    let clean = name_or_hex.trim();
    if let Some(rgb) = color_rgb(clean) {
        let hex = format!("#{:02X}{:02X}{:02X}", rgb.0, rgb.1, rgb.2);
        return Some((rgb, clean.to_lowercase(), hex));
    }
    let hex_str = clean.strip_prefix('#').unwrap_or(clean);
    if hex_str.len() == 6 {
        if let (Ok(r), Ok(g), Ok(b)) = (
            u8::from_str_radix(&hex_str[0..2], 16),
            u8::from_str_radix(&hex_str[2..4], 16),
            u8::from_str_radix(&hex_str[4..6], 16),
        ) {
            let full_hex = format!("#{r:02X}{g:02X}{b:02X}");
            return Some(((r, g, b), full_hex.clone(), full_hex));
        }
    }
    None
}

/// Переводит RGB и яркость (0..255) в формат драйвера [G, R, B] × 12.
pub fn make_ring_frame(red: u8, green: u8, blue: u8, brightness: u8) -> [u8; RING_CHANNELS] {
    let scale = |c: u8| -> u8 {
        ((c as u16 * brightness as u16) / 255) as u8
    };
    let r = scale(red);
    let g = scale(green);
    let b = scale(blue);

    let mut frame = [0u8; RING_CHANNELS];
    for pixel in frame.chunks_exact_mut(3) {
        pixel[0] = g;
        pixel[1] = r;
        pixel[2] = b;
    }
    frame
}

fn hsv_to_rgb(h: f64, s: f64, v: f64) -> (u8, u8, u8) {
    let i = (h * 6.0).floor() as i32;
    let f = h * 6.0 - i as f64;
    let p = v * (1.0 - s);
    let q = v * (1.0 - f * s);
    let t = v * (1.0 - (1.0 - f) * s);
    let (r, g, b) = match i % 6 {
        0 => (v, t, p),
        1 => (q, v, p),
        2 => (p, v, t),
        3 => (p, q, v),
        4 => (t, p, v),
        _ => (v, p, q),
    };
    (
        (r * 255.0).clamp(0.0, 255.0).round() as u8,
        (g * 255.0).clamp(0.0, 255.0).round() as u8,
        (b * 255.0).clamp(0.0, 255.0).round() as u8,
    )
}

fn compute_animated_ring(
    mode: &str,
    base_rgb: (u8, u8, u8),
    brightness: u8,
    speed: u8,
    elapsed_secs: f64,
) -> [u8; RING_CHANNELS] {
    let speed_mult = (speed.max(1) as f64) / 25.0;
    let t = elapsed_secs * speed_mult;
    let mut frame = [0u8; RING_CHANNELS];

    if mode == "gradient" {
        for i in 0..12 {
            let hue = (t * 0.2 + (i as f64) / 12.0) % 1.0;
            let (r, g, b) = hsv_to_rgb(hue, 1.0, (brightness as f64) / 255.0);
            frame[i * 3] = g;
            frame[i * 3 + 1] = r;
            frame[i * 3 + 2] = b;
        }
    } else if mode == "breathing" {
        let factor = (t.sin() + 1.0) / 2.0 * 0.85 + 0.15;
        let eff_b = ((brightness as f64) * factor).clamp(0.0, 255.0).round() as u8;
        return make_ring_frame(base_rgb.0, base_rgb.1, base_rgb.2, eff_b);
    } else {
        return make_ring_frame(base_rgb.0, base_rgb.1, base_rgb.2, brightness);
    }

    frame
}

fn make_screen_frame(indices: &[usize], brightness: u8) -> [u8; SCREEN_CHANNELS] {
    let mut frame = [0u8; SCREEN_CHANNELS];
    let b = brightness.max(1);
    for &led in indices {
        if led < SCREEN_CHANNELS {
            frame[led] = b;
        }
    }
    frame
}

fn make_screen_frame_with_music(indices: &[usize], brightness: u8, _music_active: bool) -> [u8; SCREEN_CHANNELS] {
    make_screen_frame(indices, brightness)
}

/// Ночная анимация сна u . u с мягким медленным дыханием (период 4.5 с).
fn compute_sleep_frame(base_brightness: u8, elapsed_secs: f64, music_active: bool) -> [u8; SCREEN_CHANNELS] {
    let b = base_brightness.max(1) as f64;
    let phase = (elapsed_secs % 4.5) / 4.5 * 2.0 * std::f64::consts::PI;
    let factor = (phase.sin() + 1.0) / 2.0 * 0.6 + 0.4; // плавное колебание 40%..100%
    let eff_b = (b * factor).round().clamp(1.0, 255.0) as u8;
    make_screen_frame_with_music(SLEEPING_SEGMENTS, eff_b, music_active)
}

/// Дневная эмоциональная анимация: спокойные открытые глаза 0 . 0 большую часть времени,
/// естественные редкие моргания (~раз в 6-8 сек) и ненавязчивые редкие эмоции
/// (взгляд в сторону, улыбка, подмигивание раз в ~15-20 секунд, полный цикл 60 с).
/// При активной музыке отображаются стилизованные наушники на дисплее.
fn compute_emotional_eyes_frame(brightness: u8, elapsed_secs: f64, music_active: bool) -> [u8; SCREEN_CHANNELS] {
    let b = brightness.max(1);
    let t = elapsed_secs % 60.0;

    let segments = if t < 5.0 {
        ZERO_OPEN
    } else if t < 5.15 {
        BLINK_CLOSED
    } else if t < 11.0 {
        ZERO_OPEN
    } else if t < 11.15 {
        BLINK_CLOSED
    } else if t < 11.3 {
        ZERO_OPEN
    } else if t < 11.45 {
        BLINK_CLOSED // редкое быстрое двойное моргание
    } else if t < 18.0 {
        ZERO_OPEN
    } else if t < 19.3 {
        LOOK_LEFT // короткий взгляд влево (1.3 с)
    } else if t < 19.45 {
        BLINK_CLOSED
    } else if t < 27.0 {
        ZERO_OPEN
    } else if t < 27.15 {
        BLINK_CLOSED
    } else if t < 35.0 {
        ZERO_OPEN
    } else if t < 36.5 {
        SQUINT_HAPPY // редкая добрая улыбка ^ . ^ (1.5 с)
    } else if t < 36.65 {
        BLINK_CLOSED
    } else if t < 44.0 {
        ZERO_OPEN
    } else if t < 44.15 {
        BLINK_CLOSED
    } else if t < 50.0 {
        ZERO_OPEN
    } else if t < 51.3 {
        LOOK_RIGHT // короткий взгляд вправо (1.3 с)
    } else if t < 51.45 {
        BLINK_CLOSED
    } else if t < 57.0 {
        ZERO_OPEN
    } else if t < 58.0 {
        WINK_LEFT // редкое легкое подмигивание 0 . _ (1.0 с)
    } else if t < 58.15 {
        BLINK_CLOSED
    } else {
        ZERO_OPEN
    };

    make_screen_frame_with_music(segments, b, music_active)
}

#[derive(serde::Deserialize, Clone, Debug)]
struct RawLedFrame {
    duration: f64,
    #[serde(default)]
    brightness: Option<Vec<u8>>,
    #[serde(default)]
    colors: Option<Vec<String>>,
}

pub fn load_screen_animation(name: &str) -> Option<Vec<(f64, [u8; SCREEN_CHANNELS])>> {
    let filename = if name.ends_with(".json") {
        name.to_string()
    } else if name.starts_with("Eyes_") {
        format!("{name}.json")
    } else {
        match name {
            "happy" => "Eyes_happy.json".to_string(),
            "cunning" => "Eyes_cunning.json".to_string(),
            "fear" => "Eyes_fear.json".to_string(),
            "inglasses" => "Eyes_inglasses.json".to_string(),
            "interest" => "Eyes_interest.json".to_string(),
            "search" => "Eyes_search.json".to_string(),
            "surprised" => "Eyes_surprised.json".to_string(),
            "robotic" => "Eyes_robotic.json".to_string(),
            "frustration" => "Eyes_frustration.json".to_string(),
            "tennis" => "Tennis.json".to_string(),
            "speaking" => "speaking_night.json".to_string(),
            other => format!("{other}.json"),
        }
    };

    let path = format!("/usr/share/voice-agent/led/{filename}");
    let raw = fs::read_to_string(&path).ok()?;
    let frames: Vec<RawLedFrame> = serde_json::from_str(&raw).ok()?;

    let mut result = Vec::new();
    let mut total_time = 0.0;
    for f in frames {
        let dur = f.duration.max(0.02);
        let mut frame_buf = [0u8; SCREEN_CHANNELS];
        if let Some(br) = f.brightness {
            for (i, &val) in br.iter().enumerate().take(SCREEN_CHANNELS) {
                frame_buf[i] = val;
            }
        }
        result.push((total_time, frame_buf));
        total_time += dur;
    }
    if let Some((_, last)) = result.last().copied() {
        result.push((total_time, last));
    }
    Some(result)
}

fn read_night_settings() -> Result<NightSettings> {
    let bytes = fs::read(SETTINGS_PATH)
        .with_context(|| format!("не удалось прочитать {SETTINGS_PATH}"))?;

    let root: Value =
        serde_json::from_slice(&bytes).context("некорректный JSON device_settings.json")?;

    let mode_settings = root
        .get("night_light_mode")
        .and_then(Value::as_object)
        .context("в device_settings.json отсутствует объект night_light_mode")?;

    let color = mode_settings
        .get("color")
        .and_then(Value::as_str)
        .context("в night_light_mode отсутствует строковое поле color")?
        .trim()
        .to_ascii_lowercase();

    let mode = mode_settings
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("static")
        .trim()
        .to_ascii_lowercase();

    let speed = mode_settings
        .get("speed")
        .and_then(Value::as_u64)
        .unwrap_or(50)
        .min(100) as u8;

    let revision = mode_settings
        .get("revision")
        .and_then(Value::as_u64);

    let (frame, known_color) = match color_rgb(&color) {
        Some((red, green, blue)) => {
            (make_ring_frame(red, green, blue, 255), true)
        }
        None => (NIGHT_FALLBACK, false),
    };

    Ok(NightSettings {
        color,
        mode,
        speed,
        revision,
        frame,
        known_color,
    })
}

fn apply_settings(shared: &Shared, settings: NightSettings) {
    let mut state = shared
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if state.settings.as_ref() == Some(&settings) {
        return;
    }

    let old_color = state
        .settings
        .as_ref()
        .map(|old| old.color.clone());

    let old_mode = state
        .settings
        .as_ref()
        .map(|old| old.mode.clone());

    let old_speed = state
        .settings
        .as_ref()
        .map(|old| old.speed);

    let color_changed = old_color.as_deref() != Some(settings.color.as_str());
    let mode_changed = old_mode.as_deref() != Some(settings.mode.as_str());
    let speed_changed = old_speed != Some(settings.speed);

    if !state.user_ring_override {
        state.night_frame = Some(settings.frame);
        state.night_mode = settings.mode.clone();
        state.night_speed = settings.speed;
    }
    state.settings = Some(settings.clone());

    // Если цвет или режим изменился при включённом ночнике и без user_override,
    // даём voice-agent 1.2 секунды на собственную отрисовку.
    if !state.user_ring_override && state.night_light && (color_changed || mode_changed) {
        state.color_grace_until = Some(Instant::now() + SETTINGS_APPLY_GRACE);
    }

    bump(&mut state);
    shared.changed.notify_one();

    if !settings.known_color {
        tracing::warn!(
            "[LedArbiter] Неизвестный цвет ночника '{}'; используется NIGHT_FALLBACK",
            settings.color
        );
    } else if color_changed {
        tracing::info!(
            "[LedArbiter] Цвет ночника изменён: {}",
            settings.color
        );
    }

    if mode_changed {
        tracing::info!(
            "[LedArbiter] Режим ночника изменён: {}",
            settings.mode
        );
    }

    if speed_changed {
        tracing::info!(
            "[LedArbiter] Скорость breathing изменена: {}",
            settings.speed
        );
    }
}

fn write_ring(fd: libc::c_int, frame: &[u8; RING_CHANNELS]) -> Result<()> {
    let mut data = *frame;

    let mut request = FrameReq {
        start: 0,
        count: RING_CHANNELS as libc::c_uint,
        data: data.as_mut_ptr(),
    };

    let result = unsafe {
        libc::ioctl(fd, RING_WRITE, &mut request)
    };

    if result < 0 {
        bail!(
            "ioctl /dev/topleds: {}",
            std::io::Error::last_os_error()
        );
    }

    Ok(())
}

fn write_screen_ioctl(fd: libc::c_int, frame: &[u8; SCREEN_CHANNELS]) -> Result<()> {
    let mut buf = [0u8; SCREEN_CHANNELS * 2];
    for (i, &val) in frame.iter().enumerate() {
        buf[i * 2] = val;
        buf[i * 2 + 1] = 0;
    }

    let mut request = ScreenFrameReq {
        start: 0,
        count: SCREEN_CHANNELS as libc::c_uint,
        data: buf.as_mut_ptr(),
    };

    let result = unsafe {
        libc::ioctl(fd, SCREEN_WRITE, &mut request)
    };

    if result < 0 {
        bail!(
            "ioctl /dev/is31fl32xx: {}",
            std::io::Error::last_os_error()
        );
    }

    Ok(())
}

fn write_screen_sysfs(
    frame: &[u8; SCREEN_CHANNELS],
    force: bool,
    previous: &mut Option<[u8; SCREEN_CHANNELS]>,
) -> bool {
    let mut all_ok = true;

    for (index, value) in frame.iter().enumerate() {
        if !force
            && previous
                .as_ref()
                .map(|old| old[index] == *value)
                .unwrap_or(false)
        {
            continue;
        }

        let path = format!(
            "/sys/class/leds/clock-led{}/brightness",
            index + 1
        );

        if let Err(error) = fs::write(&path, value.to_string()) {
            all_ok = false;
            tracing::warn!(
                "[LedArbiter] Не удалось записать {}: {}",
                path,
                error
            );
        }
    }

    if all_ok {
        *previous = Some(*frame);
    }

    all_ok
}

fn write_screen(
    screen_fd: Option<libc::c_int>,
    frame: &[u8; SCREEN_CHANNELS],
    force: bool,
    previous: &mut Option<[u8; SCREEN_CHANNELS]>,
) -> bool {
    if !force && previous.as_ref() == Some(frame) {
        return true;
    }

    if let Some(fd) = screen_fd {
        if let Err(error) = write_screen_ioctl(fd, frame) {
            tracing::warn!(
                "[LedArbiter] Ошибка ioctl /dev/is31fl32xx ({error:#}), откат на sysfs"
            );
            write_screen_sysfs(frame, force, previous)
        } else {
            *previous = Some(*frame);
            true
        }
    } else {
        write_screen_sysfs(frame, force, previous)
    }
}

fn writer_loop(shared: Arc<Shared>, ring_device: File, screen_device: Option<File>) {
    let ring_fd = ring_device.as_raw_fd();
    let screen_fd = screen_device.as_ref().map(|f| f.as_raw_fd());
    let start_time = Instant::now();

    let mut next_settings_poll = Instant::now();
    let mut next_lux_check = Instant::now();
    let mut settings_unavailable_logged = false;

    let mut previous_screen: Option<[u8; SCREEN_CHANNELS]> = None;
    let mut last_screen_attempt: Option<Instant> = None;

    loop {
        let now = Instant::now();

        if now >= next_settings_poll {
            match read_night_settings() {
                Ok(settings) => {
                    settings_unavailable_logged = false;
                    apply_settings(&shared, settings);
                }
                Err(error) => {
                    let mut state = shared
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());

                    if state.settings.is_none() && !settings_unavailable_logged {
                        state.night_frame = Some(NIGHT_FALLBACK);
                        state.night_mode = "static".to_owned();
                        bump(&mut state);
                        shared.changed.notify_one();

                        tracing::warn!(
                            "[LedArbiter] Не удалось прочитать настройки ночника: {error:#}; используется NIGHT_FALLBACK"
                        );

                        settings_unavailable_logged = true;
                    }
                }
            }

            next_settings_poll = Instant::now() + SETTINGS_POLL_INTERVAL;
        }

        if now >= next_lux_check {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if state.auto_brightness && state.screen_mode == "clock" {
                if let Ok(lux) = crate::voice_agent_lapi::read_ambient_lux() {
                    state.ambient_lux = lux;
                    let target_b = if lux < 0.2 {
                        12
                    } else if lux < 1.0 {
                        25
                    } else if lux < 10.0 {
                        60
                    } else if lux < 50.0 {
                        120
                    } else if lux < 150.0 {
                        180
                    } else {
                        255
                    };
                    let cur = state.screen_brightness as f32;
                    let tgt = target_b as f32;
                    let next = (cur * 0.7 + tgt * 0.3).round() as u8;
                    state.screen_brightness = next.clamp(10, 255);
                }
            }
            next_lux_check = Instant::now() + Duration::from_millis(1500);
        }

        let (
            generation,
            frame,
            flood,
            blanking,
            is_anim,
            anim_mode,
            anim_base_rgb,
            anim_brightness,
            anim_speed,
            screen_mode,
            screen_brightness,
            is_screen_anim,
            interacting,
            screen_frame,
            emotion_frame,
            pending_frame,
            music_active,
            stopping,
        ) = {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            let now = Instant::now();

            if state
                .blank_until
                .map(|until| now >= until)
                .unwrap_or(false)
            {
                state.blank_until = None;
                bump(&mut state);
                tracing::info!("[LedArbiter] Временный blank завершён");
            }

            if state
                .color_grace_until
                .map(|until| now >= until)
                .unwrap_or(false)
            {
                state.color_grace_until = None;
                bump(&mut state);
                tracing::debug!(
                    "[LedArbiter] Окно применения цвета завершено; static ночник передан арбитру"
                );
            }

            let blanking = blank_active(&state, now);

            if state.stopping {
                (
                    state.generation,
                    [0; RING_CHANNELS],
                    false,
                    false,
                    false,
                    String::new(),
                    (0, 0, 0),
                    0,
                    0,
                    state.screen_mode.clone(),
                    state.screen_brightness,
                    false,
                    false,
                    state.screen_frame,
                    None,
                    None,
                    false,
                    true,
                )
            } else {
                let fl = flood_active(&state, now);
                let is_anim = fl && state.user_ring_override && (state.night_mode == "gradient" || state.night_mode == "breathing");
                let is_screen_anim = state.screen_mode == "eyes" || state.screen_mode == "sleep" || state.active_emotion.is_some();
                let interacting = interacting_active(&state, now);

                let emotion_frame = if let Some((ref _name, start, deadline)) = state.active_emotion {
                    if now >= deadline {
                        state.active_emotion = None;
                        state.emotion_frames = None;
                        None
                    } else if let Some(ref frames) = state.emotion_frames {
                        if frames.is_empty() {
                            None
                        } else {
                            let total_dur = frames.last().map(|(t, _)| *t).unwrap_or(1.0).max(0.05);
                            let elapsed = now.duration_since(start).as_secs_f64() % total_dur;
                            let mut selected = &frames[0].1;
                            for (t, f) in frames {
                                if *t <= elapsed {
                                    selected = f;
                                } else {
                                    break;
                                }
                            }
                            let b = state.screen_brightness.max(1);
                            let mut scaled = [0u8; SCREEN_CHANNELS];
                            for i in 0..SCREEN_CHANNELS {
                                if selected[i] > 0 {
                                    scaled[i] = b;
                                }
                            }
                            Some(scaled)
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                (
                    state.generation,
                    ring_frame(&state),
                    fl,
                    blanking,
                    is_anim,
                    state.night_mode.clone(),
                    state.user_base_rgb,
                    state.night_brightness,
                    state.night_speed,
                    state.screen_mode.clone(),
                    state.screen_brightness,
                    is_screen_anim,
                    interacting,
                    state.screen_frame,
                    emotion_frame,
                    state.pending_frame.take(),
                    state.music_active,
                    false,
                )
            }
        };

        if stopping {
            let _ = write_ring(ring_fd, &[0; RING_CHANNELS]);
            let blank = [0u8; SCREEN_CHANNELS];
            write_screen(screen_fd, &blank, true, &mut previous_screen);
            break;
        }

        // Экран: гашение при переключении / режим off / глазки / часы / кастомный кадр
        if blanking {
            // Во время переключения трека или смены аудиопотока полностью гасим экран Маруси!
            let blank = [0u8; SCREEN_CHANNELS];
            write_screen(screen_fd, &blank, true, &mut previous_screen);
        } else if screen_mode == "off" {
            // В режиме выключенного экрана непрерывно держим строгие нули
            let blank = [0u8; SCREEN_CHANNELS];
            let retry_due = last_screen_attempt
                .map(|at| at.elapsed() >= SCREEN_RETRY_INTERVAL)
                .unwrap_or(true);
            let changed = previous_screen.as_ref() != Some(&blank);
            if changed || retry_due {
                last_screen_attempt = Some(Instant::now());
                write_screen(screen_fd, &blank, true, &mut previous_screen);
            }
        } else if let Some(e_frame) = emotion_frame {
            let changed = previous_screen.as_ref() != Some(&e_frame);
            if changed {
                write_screen(screen_fd, &e_frame, true, &mut previous_screen);
            }
        } else if is_screen_anim {
            if interacting {
                // Если идёт голосовой диалог или регулировка громкости:
                // полностью уступаем экран voice-agent
                if previous_screen.is_some() {
                    previous_screen = None;
                }
                thread::sleep(Duration::from_millis(60));
                continue;
            }

            let is_sleeping = screen_mode == "sleep" || (screen_mode == "eyes" && is_night_time());
            let target_frame = if is_sleeping {
                compute_sleep_frame(screen_brightness, start_time.elapsed().as_secs_f64(), music_active)
            } else {
                compute_emotional_eyes_frame(screen_brightness, start_time.elapsed().as_secs_f64(), music_active)
            };

            let changed = previous_screen.as_ref() != Some(&target_frame);
            let retry_due = last_screen_attempt
                .map(|at| at.elapsed() >= SCREEN_RETRY_INTERVAL)
                .unwrap_or(true);

            if changed || retry_due {
                let force = retry_due || previous_screen.is_none();
                last_screen_attempt = Some(Instant::now());
                write_screen(screen_fd, &target_frame, force, &mut previous_screen);
            }
        } else if let Some(screen) = screen_frame {
            let changed = previous_screen.as_ref() != Some(&screen);
            let retry_due = last_screen_attempt
                .map(|at| at.elapsed() >= SCREEN_RETRY_INTERVAL)
                .unwrap_or(true);

            if changed || retry_due {
                let force = retry_due || previous_screen.is_none();
                last_screen_attempt = Some(Instant::now());
                write_screen(screen_fd, &screen, force, &mut previous_screen);
            }
        } else if previous_screen.is_some() {
            // Возврат в штатный режим "clock" из кастомного (глазки/сон/solid/blank):
            // однократно гасим кастомные сегменты, чтобы на экране не оставалось залипших светодиодов
            let blank = [0u8; SCREEN_CHANNELS];
            write_screen(screen_fd, &blank, true, &mut previous_screen);
            previous_screen = None;
        }

        // Однократный кадр
        if let Some(pending) = pending_frame {
            let still_current = {
                let state = shared
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());

                state.generation == generation
            };

            if still_current {
                if let Err(error) = write_ring(ring_fd, &pending) {
                    tracing::error!(
                        "[LedArbiter] Ошибка записи pending-кадра: {error:#}"
                    );
                }
            }

            continue;
        }

        if is_anim {
            let anim_frame = compute_animated_ring(
                &anim_mode,
                anim_base_rgb,
                anim_brightness,
                anim_speed,
                start_time.elapsed().as_secs_f64(),
            );
            if let Err(error) = write_ring(ring_fd, &anim_frame) {
                tracing::error!("[LedArbiter] Ошибка записи анимации: {error:#}");
            }
            thread::sleep(Duration::from_millis(30));
            continue;
        }

        if flood {
            let mut interrupted = false;

            for _ in 0..FLOOD_BATCH {
                let still_current = {
                    let state = shared
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());

                    !state.stopping && state.generation == generation
                };

                if !still_current {
                    interrupted = true;
                    break;
                }

                if let Err(error) = write_ring(ring_fd, &frame) {
                    tracing::error!(
                        "[LedArbiter] Flood ioctl завершился ошибкой: {error:#}"
                    );
                    thread::sleep(Duration::from_millis(100));
                    interrupted = true;
                    break;
                }

                if blanking {
                    let blank = [0u8; SCREEN_CHANNELS];
                    let _ = write_screen(screen_fd, &blank, true, &mut previous_screen);
                }
            }

            if interrupted {
                continue;
            }

            if is_screen_anim {
                thread::sleep(Duration::from_millis(60));
            }

            continue;
        }

        if is_screen_anim {
            thread::sleep(Duration::from_millis(60));
            continue;
        }

        let state = shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.stopping {
            break;
        }

        let until_poll = next_settings_poll
            .saturating_duration_since(Instant::now());

        let wait = until_poll.min(IDLE_WAIT);

        let (_guard, _) = shared
            .changed
            .wait_timeout(state, wait)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }

    tracing::info!("[LedArbiter] Единственный LED writer остановлен");
}

impl LedArbiter {
    pub fn start() -> Result<()> {
        if GLOBAL.get().is_some() {
            return Ok(());
        }

        let device = OpenOptions::new()
            .write(true)
            .open(RING_DEVICE)
            .with_context(|| format!("не удалось открыть {RING_DEVICE}"))?;

        let screen_device = OpenOptions::new()
            .read(true)
            .write(true)
            .open(SCREEN_DEVICE)
            .map_err(|e| {
                tracing::warn!(
                    "[LedArbiter] Не удалось открыть {SCREEN_DEVICE}: {e:#}; используется sysfs fallback"
                );
                e
            })
            .ok();

        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                night_frame: Some(NIGHT_FALLBACK),
                night_mode: "static".to_owned(),
                night_speed: 50,
                night_brightness: 255,
                user_color_name: "purple".to_owned(),
                user_base_rgb: (128, 0, 128),
                screen_mode: "eyes".to_owned(),
                screen_brightness: 5,
                pending_frame: Some([0; RING_CHANNELS]),
                ..State::default()
            }),
            changed: Condvar::new(),
        });

        let thread_shared = shared.clone();

        let worker = thread::Builder::new()
            .name("capsula-led-arbiter".to_owned())
            .spawn(move || writer_loop(thread_shared, device, screen_device))
            .context("не удалось запустить LED writer-поток")?;

        GLOBAL
            .set(LedArbiter {
                shared,
                thread: Mutex::new(Some(worker)),
            })
            .map_err(|_| anyhow::anyhow!("LED arbiter уже запущен"))?;

        tracing::info!(
            "[LedArbiter] Запущен: ring ioctl 36ch, screen ioctl 30ch (sysfs fallback), settings poll 150ms"
        );

        Ok(())
    }

    fn global() -> Result<&'static LedArbiter> {
        GLOBAL.get().context("LED arbiter ещё не запущен")
    }

    fn update<F>(&self, update: F)
    where
        F: FnOnce(&mut State),
    {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        update(&mut state);
        bump(&mut state);
        self.shared.changed.notify_one();
    }

    pub fn get_status(&self) -> Result<(Value, Value)> {
        let state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        let (color, hex, mode, brightness, speed) = if state.user_ring_override {
            let hex = format!("#{:02X}{:02X}{:02X}", state.user_base_rgb.0, state.user_base_rgb.1, state.user_base_rgb.2);
            (state.user_color_name.clone(), hex, state.night_mode.clone(), state.night_brightness, state.night_speed)
        } else if let Some(ref s) = state.settings {
            let rgb = color_rgb(&s.color).unwrap_or((128, 0, 128));
            let hex = format!("#{:02X}{:02X}{:02X}", rgb.0, rgb.1, rgb.2);
            (s.color.clone(), hex, s.mode.clone(), 255, s.speed)
        } else {
            ("purple".to_string(), "#800080".to_string(), "static".to_string(), 255, 50)
        };

        let ring = serde_json::json!({
            "night_light": state.night_light,
            "color": color,
            "hex": hex,
            "mode": mode,
            "brightness": brightness,
            "speed": speed,
            "user_override": state.user_ring_override,
        });

        let is_night = is_night_time();
        let screen = serde_json::json!({
            "mode": state.screen_mode,
            "brightness": state.screen_brightness,
            "is_night": is_night,
            "auto_brightness": state.auto_brightness,
            "ambient_lux": state.ambient_lux,
            "active_emotion": state.active_emotion.as_ref().map(|(e, _, _)| e.clone()),
        });

        Ok((ring, screen))
    }

    pub fn trigger_emotion(&self, emotion: &str, duration: Option<Duration>) -> Result<()> {
        let dur = duration.unwrap_or(Duration::from_millis(3500));
        let frames = load_screen_animation(emotion);
        let start = Instant::now();
        let deadline = start + dur;
        self.update(|state| {
            state.active_emotion = Some((emotion.to_string(), start, deadline));
            state.emotion_frames = frames;
        });
        tracing::info!("[LedArbiter] Запущена эмоция: '{emotion}' на {} мс", dur.as_millis());
        Ok(())
    }

    pub fn set_auto_brightness(&self, enabled: bool) -> Result<()> {
        self.update(|state| {
            state.auto_brightness = enabled;
        });
        tracing::info!("[LedArbiter] Автояркость экрана: {}", if enabled { "ВКЛ" } else { "ВЫКЛ" });
        Ok(())
    }

    pub fn set_ring(
        &self,
        night_light: Option<bool>,
        color: Option<String>,
        mode: Option<String>,
        brightness: Option<u8>,
        speed: Option<u8>,
    ) -> Result<()> {
        self.update(|state| {
            state.user_ring_override = true;
            if let Some(nl) = night_light {
                state.night_light = nl;
                if !nl {
                    state.pending_frame = Some([0; RING_CHANNELS]);
                }
            } else {
                state.night_light = true;
            }

            if let Some(c) = color {
                if let Some((rgb, name, _)) = parse_color(&c) {
                    state.user_base_rgb = rgb;
                    state.user_color_name = name;
                }
            }

            if let Some(m) = mode {
                let m_clean = m.trim().to_ascii_lowercase();
                if m_clean == "off" {
                    state.night_light = false;
                    state.night_mode = "off".to_string();
                    state.pending_frame = Some([0; RING_CHANNELS]);
                } else {
                    state.night_mode = m_clean;
                }
            }

            if let Some(b) = brightness {
                state.night_brightness = b;
            }

            if let Some(s) = speed {
                state.night_speed = s;
            }

            let eff_frame = make_ring_frame(
                state.user_base_rgb.0,
                state.user_base_rgb.1,
                state.user_base_rgb.2,
                state.night_brightness,
            );
            state.night_frame = Some(eff_frame);
        });

        tracing::info!("[LedArbiter] Ручная настройка кольца применена");
        Ok(())
    }

    pub fn reset_ring_override(&self) -> Result<()> {
        self.update(|state| {
            state.user_ring_override = false;
            if let Some(ref s) = state.settings {
                state.night_frame = Some(s.frame);
                state.night_mode = s.mode.clone();
                state.night_speed = s.speed;
            }
        });
        tracing::info!("[LedArbiter] Сброшен пользовательский оверрайд кольца (возврат к настройкам Маруси)");
        Ok(())
    }

    pub fn set_screen(&self, mode: &str, brightness: Option<u8>) -> Result<()> {
        let mode_clean = mode.trim().to_ascii_lowercase();
        self.update(|state| {
            if let Some(b) = brightness {
                state.screen_brightness = b;
            }
            let b = state.screen_brightness;
            state.screen_mode = mode_clean.clone();

            match mode_clean.as_str() {
                "clock" => {
                    state.screen_frame = None;
                    state.active_emotion = None;
                }
                "off" => {
                    state.screen_frame = Some([0; SCREEN_CHANNELS]);
                    state.active_emotion = None;
                }
                "eyes" | "sleep" => {
                    // Анимированные глазки и ночной сон вычисляются динамически
                    state.screen_frame = None;
                    state.active_emotion = None;
                }
                "solid" => {
                    state.screen_frame = Some([b; SCREEN_CHANNELS]);
                    state.active_emotion = None;
                }
                emotion => {
                    state.screen_frame = None;
                    let frames = load_screen_animation(emotion);
                    let start = Instant::now();
                    let deadline = start + Duration::from_millis(3500);
                    state.active_emotion = Some((emotion.to_string(), start, deadline));
                    state.emotion_frames = frames;
                }
            }
        });
        tracing::info!("[LedArbiter] Режим экрана установлен: '{}'", mode_clean);
        Ok(())
    }

    pub fn set_night_light(&self, on: bool) {
        self.update(|state| {
            state.night_light = on;

            if on {
                state.pending_frame = None;
            } else {
                state.interacting_until = None;
                state.color_grace_until = None;
                state.pending_frame = Some([0; RING_CHANNELS]);
            }
        });

        tracing::info!(
            "[LedArbiter] NightLight={}",
            if on { "ON" } else { "OFF" }
        );
    }

    pub fn activity(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }

        let deadline = Instant::now() + duration;

        self.update(|state| {
            state.interacting_until = Some(
                state
                    .interacting_until
                    .map(|current| current.max(deadline))
                    .unwrap_or(deadline),
            );
        });

        tracing::debug!(
            "[LedArbiter] Voice activity продлена на {} мс",
            duration.as_millis()
        );
    }

    pub fn begin_blank(&self, duration: Duration) {
        if duration.is_zero() {
            self.end_blank();
            return;
        }

        let duration = duration.min(MAX_BLANK);

        self.update(|state| {
            state.blank_until = Some(Instant::now() + duration);
            state.interacting_until = None;
        });

        tracing::info!(
            "[LedArbiter] Blank запущен на {} мс",
            duration.as_millis()
        );
    }

    pub fn end_blank(&self) {
        self.update(|state| {
            state.blank_until = None;
        });

        tracing::debug!("[LedArbiter] Blank завершён");
    }

    pub fn render_screen(&self, frame: [u8; SCREEN_CHANNELS]) {
        self.update(|state| {
            state.screen_frame = Some(frame);
        });
    }

    pub fn show_squint(&self, duration: Duration) {
        let (brightness, music_active) = {
            let s = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
            (s.screen_brightness, s.music_active)
        };
        let frame = make_screen_frame_with_music(SQUINT_HAPPY, brightness, music_active);
        self.render_screen(frame);
        let shared = self.shared.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            let mut state = shared.state.lock().unwrap_or_else(|p| p.into_inner());
            state.screen_frame = None;
            bump(&mut state);
            shared.changed.notify_all();
        });
    }

    pub fn set_music_active(&self, active: bool) {
        self.update(|state| {
            state.music_active = active;
        });
        tracing::debug!("[LedArbiter] Music active={active}");
    }

    pub fn shutdown(&self) {
        self.update(|state| {
            state.stopping = true;
        });

        if let Some(worker) = self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = worker.join();
        }
    }
}

pub fn start() -> Result<()> {
    LedArbiter::start()
}

pub fn get_status() -> Result<(Value, Value)> {
    LedArbiter::global()?.get_status()
}

pub fn set_ring(
    night_light: Option<bool>,
    color: Option<String>,
    mode: Option<String>,
    brightness: Option<u8>,
    speed: Option<u8>,
) -> Result<()> {
    LedArbiter::global()?.set_ring(night_light, color, mode, brightness, speed)
}

pub fn reset_ring_override() -> Result<()> {
    LedArbiter::global()?.reset_ring_override()
}

pub fn set_screen(mode: &str, brightness: Option<u8>) -> Result<()> {
    LedArbiter::global()?.set_screen(mode, brightness)
}

pub fn trigger_emotion(emotion: &str, duration: Option<Duration>) -> Result<()> {
    LedArbiter::global()?.trigger_emotion(emotion, duration)
}

pub fn set_auto_brightness(enabled: bool) -> Result<()> {
    LedArbiter::global()?.set_auto_brightness(enabled)
}

pub fn set_night_light(on: bool) -> Result<()> {
    LedArbiter::global()?.set_night_light(on);
    Ok(())
}

pub fn activity(duration: Duration) -> Result<()> {
    LedArbiter::global()?.activity(duration);
    Ok(())
}

pub fn begin_blank(duration: Duration) -> Result<()> {
    LedArbiter::global()?.begin_blank(duration);
    Ok(())
}

pub fn end_blank() -> Result<()> {
    LedArbiter::global()?.end_blank();
    Ok(())
}

pub fn show_squint(duration: Duration) -> Result<()> {
    LedArbiter::global()?.show_squint(duration);
    Ok(())
}

pub fn set_music_active(active: bool) -> Result<()> {
    LedArbiter::global()?.set_music_active(active);
    Ok(())
}

pub fn render_screen(frame: [u8; SCREEN_CHANNELS]) -> Result<()> {
    LedArbiter::global()?.render_screen(frame);
    Ok(())
}

pub fn shutdown() {
    if let Some(arbiter) = GLOBAL.get() {
        arbiter.shutdown();
    }
}
