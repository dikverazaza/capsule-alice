#!/bin/sh
#
# /usr/bin/wifi_setup_helper.sh
# Assistant and Fail-safe Watchdog for Setup Hotspot & Wi-Fi roaming on VK Capsule Mini
#

SETUP_AUDIO="/usr/share/sounds/setup_instruction.wav"
STATE_FILE="/tmp/wifi_setup.state"

get_ip() {
    ip -4 addr show wlan0 2>/dev/null | grep -o 'inet [0-9.]*' | cut -d' ' -f2
}

has_internet() {
    ip route show default 2>/dev/null | grep -q 'default via'
}

get_current_ssid() {
    wpa_cli status 2>/dev/null | grep '^ssid=' | cut -d= -f2
}

play_audio() {
    file="$1"
    [ -f "$file" ] || return 0
    SOCK=$(grep -o '/tmp/pulse-[^/]*/native' /proc/net/unix 2>/dev/null | head -n 1)
    if [ -n "$SOCK" ]; then
        PULSE_SERVER="unix:$SOCK" paplay --device=master "$file" >/dev/null 2>&1
    fi
}

echo "Starting Wi-Fi Setup Helper..."
echo "STARTING" > "$STATE_FILE"

# 1. Boot check: give up to 15 seconds to connect to either Setup or home Wi-Fi
CONNECTED=0
for i in $(seq 1 15); do
    IP=$(get_ip)
    if [ -n "$IP" ] && has_internet; then
        CONNECTED=1
        echo "CONNECTED" > "$STATE_FILE"
        echo "Wi-Fi is connected on boot: SSID=$(get_current_ssid) IP=$IP"
        break
    fi
    sleep 1
done

# 2. If disconnected after 15 seconds, enter interactive setup guidance mode
if [ $CONNECTED -eq 0 ]; then
    echo "NEED_SETUP" > "$STATE_FILE"
    echo "night_color 255 140 0 0.8" > /tmp/ring.fifo 2>/dev/null
    sleep 1
    play_audio "$SETUP_AUDIO"
fi

# 3. Persistent loop: Fail-safe watchdog + Roaming to Setup priority network
PROMPT_COOLDOWN=0
while true; do
    IP=$(get_ip)
    CURRENT_SSID=$(get_current_ssid)

    if [ -n "$IP" ] && has_internet; then
        if [ "$CONNECTED" -eq 0 ]; then
            CONNECTED=1
            echo "CONNECTED" > "$STATE_FILE"
            echo "Connected to Wi-Fi: SSID=$CURRENT_SSID IP=$IP"
            echo "off" > /tmp/ring.fifo 2>/dev/null
            # Completely silent: sound "Успешно подключено к Wi-Fi" removed per user request
        fi

        # If currently connected to home Wi-Fi (not Setup), check if 'Setup' hotspot has appeared
        if [ "$CURRENT_SSID" != "Setup" ]; then
            wpa_cli scan >/dev/null 2>&1
            sleep 2
            if wpa_cli scan_results 2>/dev/null | grep -E '[[:space:]]Setup[[:space:]]*$' >/dev/null; then
                echo "Setup hotspot detected nearby! Reassociating to prefer Setup..."
                wpa_cli reassociate >/dev/null 2>&1
                sleep 2
                dhcpcd -n wlan0 >/dev/null 2>&1
            fi
        fi
    else
        # Not connected or lost connection
        CONNECTED=0
        echo "DISCONNECTED" > "$STATE_FILE"

        # Check if 'Setup' or configured Wi-Fi is available
        wpa_cli scan >/dev/null 2>&1
        sleep 2
        wpa_cli reassociate >/dev/null 2>&1
        sleep 2
        dhcpcd -n wlan0 >/dev/null 2>&1

        PROMPT_COOLDOWN=$((PROMPT_COOLDOWN + 15))
        if [ $PROMPT_COOLDOWN -ge 90 ]; then
            PROMPT_COOLDOWN=0
            echo "night_color 255 140 0 0.8" > /tmp/ring.fifo 2>/dev/null
            play_audio "$SETUP_AUDIO"
        fi
    fi

    sleep 15
done
