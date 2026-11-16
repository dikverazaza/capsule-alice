#!/bin/sh
# /usr/bin/capsula-alice-launcher.sh
# Master supervisor for Alice on VK Capsule Mini

mkdir -p /run/capsula-yandex /data/capsula-yandex /tmp
chmod 777 /run/capsula-yandex /data/capsula-yandex /tmp

# Stop original voice-agent if present
killall -9 voice-agent 2>/dev/null

# Clean old triggers and FIFOs
rm -f /tmp/ring.fifo /tmp/wakeword.trigger /tmp/alice_busy /run/capsula-yandex/control
mkfifo /tmp/ring.fifo 2>/dev/null
mkfifo /run/capsula-yandex/control 2>/dev/null
chmod 666 /tmp/ring.fifo /run/capsula-yandex/control 2>/dev/null

# Wait up to 30 seconds for PulseAudio
PULSE_SOCK=""
for i in $(seq 1 30); do
    PULSE_SOCK=$(find /tmp -name "native" -path "*/pulse-*/*" 2>/dev/null | head -n 1)
    if [ -n "$PULSE_SOCK" ]; then
        break
    fi
    sleep 1
done

if [ -n "$PULSE_SOCK" ]; then
    export PULSE_SERVER="unix:$PULSE_SOCK"
fi

# Set default comfortable volume (35%)
pactl set-sink-mute master 0 2>/dev/null
pactl set-sink-volume master 35% 2>/dev/null

# Ensure credentials exist in /data/capsula-yandex/credentials.json
if [ ! -f /data/capsula-yandex/credentials.json ] && [ -f /etc/alice/yandex_credentials.json ]; then
    cp /etc/alice/yandex_credentials.json /data/capsula-yandex/credentials.json
    chmod 600 /data/capsula-yandex/credentials.json
fi

# Start LED ring daemon & Avahi mDNS
killall -9 ring_daemon 2>/dev/null
/etc/init.d/S50avahi-daemon start 2>/dev/null
/usr/bin/ring_daemon &
sleep 1

# Start Onboarding Web Portal on ports 80 & 8080
killall -9 capsule_onboarding 2>/dev/null
/usr/bin/capsule_onboarding > /tmp/capsule_onboarding.log 2>&1 &

# Start Wi-Fi Setup Helper (watchdog and hotspot roaming)
killall -9 wifi_setup_helper.sh 2>/dev/null
/usr/bin/wifi_setup_helper.sh > /tmp/wifi_setup.log 2>&1 &

# Start Yandex Music player daemon
killall -9 capsula-yandex 2>/dev/null
/usr/bin/capsula-yandex > /tmp/capsula-yandex.log 2>&1 &
sleep 1

# Start Wakeword spotter daemon (neural model with 0.670 threshold)
killall -9 wakewordd parec 2>/dev/null
/usr/bin/wakewordd /data/model.tflite > /tmp/wakewordd.log 2>&1 &
sleep 1

# Start Capsula Alice assistant (Uniproxy + Clock + Auto-brightness)
killall -9 capsula-alice 2>/dev/null
/usr/bin/capsula-alice > /tmp/capsula-alice.log 2>&1 &

echo "Alice supervisor successfully launched all modules."
