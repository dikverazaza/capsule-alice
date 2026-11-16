//! Совместимый фасад старого RingBlank API.
//! Реальная запись кольца принадлежит только led_arbiter.

use anyhow::Result;
use std::time::Duration;

const NIGHT_STATE: &str = "/data/capsula-hub/night_light";

pub fn begin_for(duration: Duration) -> Result<()> {
    crate::led_arbiter::begin_blank(duration)
}

pub fn end() {
    if let Err(error) = crate::led_arbiter::end_blank() {
        tracing::warn!("[RingBlank] Не удалось завершить blank: {error:#}");
    }
}

pub fn night_light() -> bool {
    std::fs::read_to_string(NIGHT_STATE)
        .map(|text| text.trim() == "1")
        .unwrap_or(false)
}

pub fn set_night_light(on: bool) {
    if let Err(error) = std::fs::write(NIGHT_STATE, if on { "1\n" } else { "0\n" }) {
        tracing::warn!("[RingBlank] Не удалось сохранить NightLight: {error}");
    }

    if let Err(error) = crate::led_arbiter::set_night_light(on) {
        tracing::warn!("[RingBlank] Не удалось изменить NightLight: {error:#}");
    }
}

pub fn restore_night_light() {
    set_night_light(night_light());
}
