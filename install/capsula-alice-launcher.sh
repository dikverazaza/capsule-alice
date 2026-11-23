#!/bin/sh
# /usr/bin/capsula-alice-launcher.sh
# Master supervisor for Alice on VK Capsule Mini.
# Services are started with nohup so they survive ADB shell/session exit.

mkdir -p /run/capsula-yandex /data/capsula-yandex /var/run /tmp
chmod 777 /run/capsula-yandex /data/capsula-yandex /tmp

# Both S50avahi and S97voice-agent may call this launcher at boot.
# Only one supervisor may own the FIFO and the capture processes.
LAUNCH_LOCK=/var/run/capsula-alice-launcher.lock
if [ -d "$LAUNCH_LOCK" ]; then
    old_pid=$(cat "$LAUNCH_LOCK/pid" 2>/dev/null || printf '')
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
        exit 0
    fi
    rm -rf "$LAUNCH_LOCK" 2>/dev/null
fi
mkdir -p "$LAUNCH_LOCK" || exit 0
printf '%s\n' "$$" > "$LAUNCH_LOCK/pid" 2>/dev/null
trap 'rm -rf "$LAUNCH_LOCK" 2>/dev/null' EXIT HUP INT TERM

# Only the primary capsule carries the Alice identity flag. The second
# capsule keeps its factory hostname for separate mDNS/Bluetooth identity.
if [ -f /etc/alice-identity ]; then
    hostname alice 2>/dev/null
    echo alice > /etc/hostname 2>/dev/null
fi

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

# Never raise the speaker volume. Only cap it when it is already above 20%.
# The user may be asleep when OTA restarts services.
_vol=$(pactl get-sink-volume master 2>/dev/null | grep -o '[0-9]*%' | head -n 1 | tr -d '%')
case "$_vol" in
    ''|*[!0-9]*) ;;
    *) [ "$_vol" -gt 20 ] && pactl set-sink-volume master 20% 2>/dev/null ;;
esac
# speex mic at 100% required shouting; 250% saturated the noise floor. 150% is the balance.
pactl set-source-mute speex 0 2>/dev/null
pactl set-source-volume speex 150% 2>/dev/null

# Ensure credentials exist in /data/capsula-yandex/credentials.json
if [ ! -f /data/capsula-yandex/credentials.json ] && [ -f /etc/alice/yandex_credentials.json ]; then
    cp /etc/alice/yandex_credentials.json /data/capsula-yandex/credentials.json
    chmod 600 /data/capsula-yandex/credentials.json
fi

# Start LED ring daemon & Avahi mDNS
killall -9 ring_daemon 2>/dev/null
/etc/init.d/S50avahi-daemon start 2>/dev/null
nohup /usr/bin/ring_daemon >/tmp/ring_daemon.log 2>&1 &
sleep 1

# Start Onboarding Web Portal on ports 80 & 8080
killall -9 capsule_onboarding 2>/dev/null
nohup /usr/bin/capsule_onboarding >/tmp/capsule_onboarding.log 2>&1 &
sleep 1

# Start Wi-Fi Setup Helper (watchdog and hotspot roaming)
killall -9 wifi_setup_helper.sh 2>/dev/null
nohup /usr/bin/wifi_setup_helper.sh >/tmp/wifi_setup.log 2>&1 &
sleep 1

# Start Yandex Music player daemon
killall -9 capsula-yandex 2>/dev/null
nohup /usr/bin/capsula-yandex >/tmp/capsula-yandex.log 2>&1 &
sleep 1

# Start Wakeword spotter daemon (neural model with 0.400 threshold)
killall -9 wakewordd parec 2>/dev/null
nohup /usr/bin/wakewordd /data/model.tflite >/tmp/wakewordd.log 2>&1 &
sleep 1

# Start Capsula Alice assistant (Uniproxy + Clock + Auto-brightness)
killall -9 capsula-alice 2>/dev/null
nohup /usr/bin/capsula-alice >/tmp/capsula-alice.log 2>&1 &

echo "Alice supervisor successfully launched all modules."
