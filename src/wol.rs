use anyhow::{anyhow, Context, Result};
use crate::config::AppConfig;
use std::{net::{SocketAddrV4, UdpSocket}, str::FromStr};

pub fn magic_packet(mac: &str) -> Result<[u8; 102]> {
    let clean: String = mac.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if clean.len() != 12 { return Err(anyhow!("MAC-адрес должен содержать 12 шестнадцатеричных символов")); }
    let mut bytes = [0u8; 6];
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&clean[index * 2..index * 2 + 2], 16)
            .map_err(|_| anyhow!("Некорректный MAC-адрес"))?;
    }
    let mut packet = [0xffu8; 102];
    for repeat in 0..16 {
        let start = 6 + repeat * 6;
        packet[start..start + 6].copy_from_slice(&bytes);
    }
    Ok(packet)
}

pub fn send_wake(config: &AppConfig) -> Result<()> {
    if config.mac.is_empty() { return Err(anyhow!("Сначала укажите MAC-адрес компьютера в настройках")); }
    let packet = magic_packet(&config.mac)?;
    let broadcast = std::net::Ipv4Addr::from_str(&config.broadcast).context("Некорректный broadcast")?;
    let target = SocketAddrV4::new(broadcast, config.port);
    let socket = UdpSocket::bind("0.0.0.0:0").context("Не удалось создать UDP-сокет для WOL")?;
    socket.set_broadcast(true)?;
    // One explicit wake request produces exactly one datagram. Do not retry:
    // retries can arrive after shutdown and immediately power the PC back on.
    socket.send_to(&packet, target).context("Не удалось отправить WOL-пакет")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn builds_102_byte_magic_packet() {
        let packet = magic_packet("34:5A:60:55:B3:10").unwrap();
        assert_eq!(packet.len(), 102);
        assert_eq!(&packet[..6], &[255; 6]);
        assert_eq!(&packet[6..12], &[0x34, 0x5a, 0x60, 0x55, 0xb3, 0x10]);
    }
}
