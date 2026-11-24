#!/bin/sh
# /usr/bin/capsula-ota
# Silent OTA updater for VK Capsule Mini (Buildroot, BusyBox tools only).
#
# Commands:
#   check     download manifest, verify, install if version differs (default)
#   force     same as check but ignores the locally installed version
#   status    print current/manifest version and last result
#   rollback  restore the newest local backup
#   daemon    run checks forever every OTA_INTERVAL seconds (default 600)
#
# No token is stored on the device. Manifest and assets are public raw files
# from the "main" branch. GitHub's raw CDN can keep serving an old manifest for
# a moved tag for a long time; main becomes current immediately after publish.
# BusyBox wget does not validate TLS certificates, so every asset is verified
# against SHA-256 + size values from the manifest before any file is replaced.

set -u

# Self-test must be side-effect-free: the normal startup below deletes $WORK,
# which would remove the staged update currently being verified.
if [ "${1:-}" = "selftest" ]; then
    # Only check prerequisites of the updater itself. Host utilities used by
    # health-check/preflight are optional on a development machine.
    command -v wget >/dev/null 2>&1 && \
    command -v sha256sum >/dev/null 2>&1 || exit 1
    exit 0
fi

# Safe defaults. /etc/capsula-ota.conf is read BEFORE any path is derived,
# so a user can override OTA_HOME, URLs, intervals and backup location.
CONF="${CAPSULA_OTA_CONF:-/etc/capsula-ota.conf}"
ENABLED=1
RESTART_SERVICES=1
DEFER_RESTART_AT_NIGHT=0
QUIET_START=23
QUIET_END=7
OTA_INTERVAL=600
OTA_RETRY_INTERVAL=60
[ -f "$CONF" ] && . "$CONF"

OTA_BASE_URL="${CAPSULA_OTA_BASE_URL:-https://raw.githubusercontent.com/dikverazaza/capsule-alice/main/ota}"
MANIFEST_URL="${CAPSULA_OTA_MANIFEST_URL:-$OTA_BASE_URL/manifest.txt}"
ASSET_PREFIX="${CAPSULA_OTA_ASSET_PREFIX:-https://raw.githubusercontent.com/dikverazaza/capsule-alice/main/ota/assets/}"

OTA_HOME="${CAPSULA_OTA_HOME:-/data/ota}"
# Staging lives in tmpfs: /data on the capsule is only ~63 MB and is often full.
WORK="/tmp/capsula-ota-work"
# If /data is nearly full, keep snapshots on tmpfs so updates still install.
BACKUP="${CAPSULA_OTA_BACKUP_HOME:-$OTA_HOME/backup}"
available_kb() {
    set -- $(df -Pk "$1" 2>/dev/null | tail -n 1)
    printf '%s\n' "${4:-0}"
}
if [ -z "${CAPSULA_OTA_BACKUP_HOME:-}" ]; then
    _ota_avail=$(available_kb "$OTA_HOME")
    case "$_ota_avail" in
        ''|*[!0-9]*) ;;
        *) [ "$_ota_avail" -lt 12000 ] && BACKUP="/tmp/capsula-ota-backup" ;;
    esac
fi
LOG_FILE="/tmp/capsula-ota.log"
PERSISTENT_LOG="$OTA_HOME/ota.log"
LAST_CHECK_FILE="$OTA_HOME/last_check"
OTA_PID_FILE="/var/run/capsula-ota.pid"

CURRENT_VERSION_FILE="$OTA_HOME/current_version"
LAST_RESULT_FILE="$OTA_HOME/last_result"
PENDING_RESTART="$OTA_HOME/restart_pending"
LOCK_DIR="$OTA_HOME/.lock"
DAEMON_LOCK="$OTA_HOME/.daemon.lock"

mkdir -p "$OTA_HOME" "$BACKUP" || exit 1
rm -rf "$WORK" 2>/dev/null
mkdir -p "$WORK" || exit 1

rotate_one_log() {
    _log="$1"
    [ -f "$_log" ] || return 0
    _size=$(wc -c < "$_log" | tr -d ' ')
    if [ "${_size:-0}" -gt 200000 ]; then
        tail -n 300 "$_log" > "$_log.rot" 2>/dev/null
        mv -f "$_log.rot" "$_log" 2>/dev/null
    fi
}

rotate_log() {
    rotate_one_log "$LOG_FILE"
    rotate_one_log "$PERSISTENT_LOG"
}

log() {
    ts=$(date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || printf 'unknown-time')
    printf '%s %s\n' "$ts" "$*" >> "$LOG_FILE" 2>/dev/null
    printf '%s %s\n' "$ts" "$*" >> "$PERSISTENT_LOG" 2>/dev/null
    printf '%s\n' "$*"
    rotate_log
}

set_result() {
    printf '%s\n' "$1" > "$LAST_RESULT_FILE" 2>/dev/null
}

die() {
    log "ERROR: $*"
    set_result "error: $*"
    exit 1
}

acquire_lock() {
    if [ -d "$LOCK_DIR" ]; then
        old_pid=$(cat "$LOCK_DIR/pid" 2>/dev/null || printf '')
        if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
            log "Another OTA run is active (pid $old_pid)."
            exit 0
        fi
        rm -rf "$LOCK_DIR" 2>/dev/null
    fi
    mkdir -p "$LOCK_DIR" || die "cannot create lock directory"
    printf '%s\n' "$$" > "$LOCK_DIR/pid"
    trap 'rm -rf "$LOCK_DIR" "$WORK" 2>/dev/null' EXIT HUP INT TERM
}

allowed_dest() {
    case "$1" in
        /usr/bin/capsula-alice|\
        /usr/bin/capsula-audio|\
        /usr/bin/capsula-yandex|\
        /usr/bin/capsula-hub|\
        /usr/bin/capsula-ota|\
        /usr/bin/capsula-alice-launcher.sh|\
        /usr/bin/wakewordd|\
        /usr/bin/ring_daemon|\
        /usr/bin/capsule_onboarding|\
        /etc/init.d/S98capsula-ota|\
        /etc/init.d/S50avahi-daemon|\
        /etc/capsula-ota.conf)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

download_to() {
    _url="$1"
    _out="$2"
    _i=1
    while [ "$_i" -le 3 ]; do
        # Do not pass -T: BusyBox wget on capsule 113 segfaults with it.
        if wget -q -O "$_out.new" "$_url" 2>>"$LOG_FILE"; then
            mv -f "$_out.new" "$_out"
            return 0
        fi
        rm -f "$_out.new" 2>/dev/null
        _i=$((_i + 1))
        [ "$_i" -le 3 ] && sleep 5
    done
    return 1
}

sha_of() {
    sha256sum "$1" 2>/dev/null | cut -d' ' -f1
}

size_of() {
    set -- $(wc -c < "$1" 2>/dev/null)
    printf '%s\n' "${1:-0}"
}

is_quiet_hour() {
    [ "$DEFER_RESTART_AT_NIGHT" = "1" ] || return 1
    hour=$(date +%H 2>/dev/null)
    [ -n "$hour" ] || return 1
    case "$hour" in 0?) hour=${hour#0} ;; esac
    if [ "$QUIET_START" -gt "$QUIET_END" ]; then
        if [ "$hour" -ge "$QUIET_START" ] || [ "$hour" -lt "$QUIET_END" ]; then
            return 0
        fi
    else
        if [ "$hour" -ge "$QUIET_START" ] && [ "$hour" -lt "$QUIET_END" ]; then
            return 0
        fi
    fi
    return 1
}

restart_services() {
    if [ "$RESTART_SERVICES" != "1" ]; then
        log "Service restart disabled by configuration."
        return 0
    fi
    if is_quiet_hour; then
        printf '%s\n' "$(date '+%Y-%m-%d %H:%M:%S' 2>/dev/null)" > "$PENDING_RESTART"
        log "Quiet hours: service restart deferred. No audio, no unmute."
        return 0
    fi
    if [ -x /usr/bin/capsula-alice-launcher.sh ]; then
        nohup /usr/bin/capsula-alice-launcher.sh >>/tmp/capsula-alice-launcher.log 2>&1 &
        log "Alice services restarted silently."
    else
        log "Launcher not found; restart skipped. New binaries apply after reboot."
    fi
    rm -f "$PENDING_RESTART" 2>/dev/null
}

prune_old_snapshots() {
    _keep="$1"
    [ -d "$BACKUP" ] || return 0
    # Keep only the two newest dated snapshots: current plus one rollback.
    # A full set of binaries is ~17 MB; five copies would eat all tmpfs RAM.
    ls -1d "$BACKUP"/202* 2>/dev/null | sort -V -r | tail -n +3 | while read -r _old; do
        [ -d "$_old" ] || continue
        [ "$_old" = "$_keep" ] && continue
        case "$(basename "$_old")" in
            manual-*) continue ;;
        esac
        log "Pruning old snapshot: $(basename "$_old")"
        rm -rf "$_old"
    done
}

health_check_services() {
    # The launcher is a one-shot starter. First wait until it actually creates
    # its lock; otherwise we could sample the OLD processes before they are
    # killed and accept a dead new stack as healthy.
    _t=0
    while [ ! -d /var/run/capsula-alice-launcher.lock ] && [ "$_t" -lt 10 ]; do
        sleep 1
        _t=$((_t + 1))
    done
    if [ ! -d /var/run/capsula-alice-launcher.lock ]; then
        log "Health check FAILED: launcher did not start."
        return 1
    fi
    # The launcher removes the lock when it has finished starting the stack.
    _t=0
    while [ -d /var/run/capsula-alice-launcher.lock ] && [ "$_t" -lt 15 ]; do
        sleep 1
        _t=$((_t + 1))
    done
    _t=0
    while [ "$_t" -lt 30 ]; do
        p1_alice=$(pidof -s capsula-alice 2>/dev/null || printf '')
        p1_wake=$(pidof -s wakewordd 2>/dev/null || printf '')
        p1_ring=$(pidof -s ring_daemon 2>/dev/null || printf '')
        if [ -n "$p1_alice" ] && [ -n "$p1_wake" ] && [ -n "$p1_ring" ]; then
            p1="${p1_alice}:${p1_wake}:${p1_ring}"
            sleep 5
            p2_alice=$(pidof -s capsula-alice 2>/dev/null || printf '')
            p2_wake=$(pidof -s wakewordd 2>/dev/null || printf '')
            p2_ring=$(pidof -s ring_daemon 2>/dev/null || printf '')
            p2="${p2_alice}:${p2_wake}:${p2_ring}"
            if [ "$p1" = "$p2" ]; then
                log "Health check passed: alice/wakewordd/ring_daemon stable ($p1)."
                return 0
            fi
            log "PID instability detected: $p1 -> $p2"
        fi
        sleep 2
        _t=$((_t + 2))
    done
    log "Health check FAILED: voice stack did not stay alive."
    return 1
}

flush_pending_restart() {
    [ -f "$PENDING_RESTART" ] || return 0
    if is_quiet_hour; then
        log "Pending restart still deferred during quiet hours."
        return 0
    fi
    log "Applying deferred service restart."
    restart_services
}

atomic_replace_file() {
    _src="$1"
    _dest="$2"
    _mode="${3:-755}"
    cp -f "$_src" "$_dest.new" 2>/dev/null || return 1
    chmod "$_mode" "$_dest.new" 2>/dev/null || {
        rm -f "$_dest.new" 2>/dev/null
        return 1
    }
    sync
    _try=1
    while [ "$_try" -le 5 ]; do
        if mv -f "$_dest.new" "$_dest" 2>/dev/null; then
            return 0
        fi
        # A live executable can reject rename with ETXTBSY. Kill the owner,
        # let the launcher notice the exit, then retry with the same inode.
        case "$_dest" in
            /usr/bin/capsula-alice) killall -9 capsula-alice 2>/dev/null ;;
            /usr/bin/wakewordd) killall -9 wakewordd 2>/dev/null ;;
            /usr/bin/capsula-yandex) killall -9 capsula-yandex 2>/dev/null ;;
            /usr/bin/ring_daemon) killall -9 ring_daemon 2>/dev/null ;;
        esac
        sleep 1
        _try=$((_try + 1))
    done
    rm -f "$_dest.new" 2>/dev/null
    return 1
}

restore_snapshot() {
    _snap="$1"
    [ -f "$_snap/files.list" ] || return 1
    # The launcher would resurrect killed services and recreate ETXTBSY.
    # Its lock is a directory, so rm -f is not enough.
    killall -9 capsula-alice-launcher.sh 2>/dev/null
    rm -rf /var/run/capsula-alice-launcher.lock 2>/dev/null
    killall -9 capsula-alice wakewordd capsula-yandex ring_daemon 2>/dev/null
    _rollback_ok=1
    while IFS='|' read -r _dest _mode; do
        [ -n "$_dest" ] || continue
        # Never roll back the updater or its restart policy: a stale client
        # can cut OTA forever, and a stale config can re-enable quiet hours.
        case "$_dest" in
            /etc/capsula-ota.conf|/usr/bin/capsula-ota|/etc/init.d/S98capsula-ota|/etc/init.d/S50avahi-daemon)
                continue
                ;;
        esac
        _name=$(basename "$_dest")
        if [ ! -f "$_snap/$_name" ]; then
            log "Rollback source missing: $_name"
            _rollback_ok=0
            continue
        fi
        if ! atomic_replace_file "$_snap/$_name" "$_dest" "${_mode:-755}"; then
            log "Rollback replace failed: $_dest"
            _rollback_ok=0
        fi
    done < "$_snap/files.list"
    sync
    [ "$_rollback_ok" = "1" ] || return 1
    if [ -d "$_snap" ]; then
        printf '%s\n' "$(basename "$_snap")" > "$CURRENT_VERSION_FILE"
    fi
    return 0
}

parse_manifest() {
    _raw="$WORK/manifest.raw"
    _clean="$WORK/files.list"
    : > "$_clean"

    head -n 1 "$MANIFEST_FILE" | tr -d '\r' > "$_raw.first"
    first=$(cat "$_raw.first")
    [ "$first" = "V|1" ] || die "unsupported manifest header: $first"

    new_version=$(grep '^VERSION|' "$MANIFEST_FILE" 2>/dev/null | head -n 1 | cut -d'|' -f2 | tr -d '\r')
    case "$new_version" in
        ''|*[!A-Za-z0-9._-]*) die "invalid version in manifest" ;;
    esac

    grep '^FILE|' "$MANIFEST_FILE" 2>/dev/null | cut -d'|' -f2-7 | tr -d '\r' > "$_raw"

    [ -s "$_raw" ] || die "manifest has no FILE entries"

    while IFS='|' read -r dest asset url sha size mode; do
        allowed_dest "$dest" || die "destination not whitelisted: $dest"
        case "$asset" in
            ''|*[!A-Za-z0-9._-]*) die "invalid asset name: $asset" ;;
        esac
        case "$url" in
            "$ASSET_PREFIX"*) ;;
            *) die "asset URL is not an allowed raw asset URL: $url" ;;
        esac
        case "$sha" in
            ????????????????????????????????????????????????????????????????) ;;
            *) die "invalid SHA-256 for $asset" ;;
        esac
        case "$size" in
            ''|*[!0-9]*) die "invalid size for $asset" ;;
        esac
        case "$mode" in
            755|644|600|700) ;;
            *) die "invalid mode for $asset" ;;
        esac
        printf '%s|%s|%s|%s|%s|%s\n' "$dest" "$asset" "$url" "$sha" "$size" "$mode" >> "$_clean"
    done < "$_raw"

    [ -s "$_clean" ] || die "no valid FILE entries"
}

ota_preflight() {
    # No route (or hotspot/setup mode) means there is nothing to download.
    # Return 0 so the daemon keeps its normal 10-minute interval instead of
    # retrying every 60 seconds and wearing out flash with log writes.
    _wifi_state=$(cat /tmp/wifi_setup.state 2>/dev/null || printf '')
    if ! ip route show default 2>/dev/null | grep -q default || \
       [ "$_wifi_state" = "NEED_SETUP" ] || [ "$_wifi_state" = "DISCONNECTED" ]; then
        log "No internet route / Wi-Fi not connected: skipping OTA check."
        set_result "skipped: no route"
        return 1
    fi

    # A dead RTC makes TLS and version comparison unreliable. Try to fix the
    # clock, but never download anything while the date is still wrong.
    _year=$(date +%Y 2>/dev/null || printf '0')
    case "$_year" in
        ''|*[!0-9]*) _year=0 ;;
    esac
    if [ "$_year" -lt 2025 ]; then
        log "RTC date $_year is invalid; trying NTP."
        ntpd -q -n -p pool.ntp.org >/dev/null 2>&1 || \
            ntpd -q -n -p time.google.com >/dev/null 2>&1
        _year=$(date +%Y 2>/dev/null || printf '0')
        case "$_year" in
            ''|*[!0-9]*) _year=0 ;;
        esac
        if [ "$_year" -lt 2025 ]; then
            log "Clock is still invalid ($_year); skipping OTA check."
            set_result "skipped: clock"
            return 1
        fi
        log "Clock synchronized: $_year."
    fi
    return 0
}

do_check() {
    force="$1"
    [ "$ENABLED" = "1" ] || { log "OTA disabled in configuration."; return 0; }

    # Network and clock guards must not create a lock directory on every
    # failed boot attempt; they run before the lock is acquired.
    ota_preflight || return 0

    acquire_lock
    # Secondary capsule waits so the primary can prove a release is good.
    # Manual checks must not block a user in the terminal for 3 minutes.
    if [ "$force" != "1" ] && [ ! -f /etc/alice-identity ]; then
        log "Secondary capsule: canary delay 180s."
        sleep 180
    fi
    date '+%Y-%m-%d %H:%M:%S' > "$LAST_CHECK_FILE" 2>/dev/null
    log "Backup location: $BACKUP"

    MANIFEST_FILE="$WORK/manifest.txt"
    # Fastly caches raw.githubusercontent by full URL. A timestamp query
    # forces a fresh manifest when a release has just been published.
    download_to "$MANIFEST_URL?cb=$(date +%s)" "$MANIFEST_FILE" || die "cannot download manifest from $MANIFEST_URL"
    log "Manifest downloaded."

    parse_manifest

    flush_pending_restart

    current_version=$(cat "$CURRENT_VERSION_FILE" 2>/dev/null || printf 'none')
    if [ "$force" != "1" ] && [ "$current_version" = "$new_version" ]; then
        log "Version $new_version already installed. Nothing to do."
        set_result "current $new_version"
        return 0
    fi
    # raw.githubusercontent may briefly serve an older manifest after the tag
    # moves. Never downgrade automatically because of that.
    if [ "$force" != "1" ] && [ "$current_version" != "none" ]; then
        highest=$(printf '%s\n%s\n' "$current_version" "$new_version" | sort -V | tail -n 1)
        if [ "$highest" = "$current_version" ]; then
            log "Manifest $new_version is older than installed $current_version; ignoring."
            set_result "current $current_version"
            return 0
        fi
    fi
    log "Update available: $current_version -> $new_version"

    # Download and verify only files that really differ from what is installed.
    # This matters on the capsule: /data is small and already nearly full.
    : > "$WORK/skip.list"
    while IFS='|' read -r dest asset url sha size mode; do
        if [ -f "$dest" ]; then
            installed_sha=$(sha_of "$dest")
            installed_size=$(size_of "$dest")
            if [ "$installed_sha" = "$sha" ] && [ "$installed_size" = "$size" ]; then
                printf '%s\n' "$asset" >> "$WORK/skip.list"
                log "Already current: $dest"
                continue
            fi
        fi
        staged="$WORK/$asset"
        # The SHA from the manifest is a stable cache-buster: a new binary
        # always gets a new URL even while raw.githubusercontent is stale.
        download_to "$url?cb=$sha" "$staged" || die "download failed: $asset"
        actual_size=$(size_of "$staged")
        [ "$actual_size" = "$size" ] || die "size mismatch for $asset: $actual_size != $size"
        actual_sha=$(sha_of "$staged")
        [ "$actual_sha" = "$sha" ] || die "SHA-256 mismatch for $asset"
        # The updater replaces itself. Never install a syntactically broken
        # client: that would disable OTA and all future rollback paths.
        if [ "$dest" = "/usr/bin/capsula-ota" ]; then
            sh -n "$staged" || die "self-test failed: broken shell syntax"
            sh "$staged" selftest || die "self-test failed: missing runtime prerequisites"
        fi
        log "Verified $asset ($actual_size bytes)."
    done < "$WORK/files.list"

    # Snapshot only files that are about to be replaced.
    snapshot="$BACKUP/$current_version"
    mkdir -p "$snapshot"
    : > "$snapshot/files.list"
    need_restart=0
    while IFS='|' read -r dest asset url sha size mode; do
        grep -Fqx "$asset" "$WORK/skip.list" >/dev/null 2>&1 && continue
        if [ -e "$dest" ]; then
            cp -p "$dest" "$snapshot/$(basename "$dest")" 2>/dev/null || die "backup failed: $dest"
        fi
        printf '%s|%s\n' "$dest" "$mode" >> "$snapshot/files.list"
        case "$dest" in
            /usr/bin/capsula-ota|/etc/init.d/S98capsula-ota|/etc/init.d/S50avahi-daemon) ;;
            *) need_restart=1 ;;
        esac
    done < "$WORK/files.list"

    installed_ok=1
    while IFS='|' read -r dest asset url sha size mode; do
        grep -Fqx "$asset" "$WORK/skip.list" >/dev/null 2>&1 && continue
        staged="$WORK/$asset"
        cp -f "$staged" "$dest.new" 2>/dev/null || { installed_ok=0; break; }
        chmod "$mode" "$dest.new" 2>/dev/null || { installed_ok=0; break; }
        sync
        # A live executable can reject rename with ETXTBSY. Stop that service
        # and retry instead of rolling back an otherwise verified update.
        _mv_ok=0
        _try=1
        while [ "$_try" -le 5 ]; do
            if mv -f "$dest.new" "$dest" 2>/dev/null; then
                _mv_ok=1
                break
            fi
            case "$dest" in
                /usr/bin/capsula-alice) killall -9 capsula-alice 2>/dev/null ;;
                /usr/bin/wakewordd) killall -9 wakewordd 2>/dev/null ;;
                /usr/bin/capsula-yandex) killall -9 capsula-yandex 2>/dev/null ;;
                /usr/bin/ring_daemon) killall -9 ring_daemon 2>/dev/null ;;
            esac
            sleep 1
            _try=$((_try + 1))
        done
        [ "$_mv_ok" = "1" ] || { installed_ok=0; break; }
        log "Installed $dest"
    done < "$WORK/files.list"

    if [ "$installed_ok" != "1" ]; then
        log "Installation failed; rolling back to $current_version."
        if restore_snapshot "$snapshot"; then
            set_result "rollback to $current_version"
        else
            set_result "rollback failed; partial restore"
        fi
        # restore_snapshot kills the launcher and service binaries. Without
        # this the capsule stays silent until a manual power cycle.
        restart_services
        exit 1
    fi

    sync
    # The update may have replaced the config file. Apply its restart policy now,
    # not on the next 10-minute check.
    [ -f "$CONF" ] && . "$CONF"
    PENDING_RESTART="$OTA_HOME/restart_pending"

    # Never mark an update installed before we know the new voice stack lives.
    if [ "$need_restart" = "1" ]; then
        restart_services
        if ! health_check_services; then
            log "Health check failed; rolling back to $current_version."
            restore_snapshot "$snapshot"
            restart_services
            set_result "rollback: health check failed"
            exit 1
        fi
    fi

    printf '%s\n' "$new_version" > "$CURRENT_VERSION_FILE"
    sync
    set_result "updated $new_version"
    log "OTA update completed: $new_version"
    prune_old_snapshots "$snapshot"
    return 0
}

acquire_daemon_lock() {
    if mkdir "$DAEMON_LOCK" 2>/dev/null; then
        printf '%s\n' "$$" > "$DAEMON_LOCK/pid"
        printf '%s\n' "$$" > "$OTA_PID_FILE" 2>/dev/null
        return 0
    fi
    old_pid=$(cat "$DAEMON_LOCK/pid" 2>/dev/null || printf '')
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
        # start-stop-daemon may have written our PID before the lock check.
        # Do not leave that stale PID behind when this duplicate exits.
        written_pid=$(cat "$OTA_PID_FILE" 2>/dev/null || printf '')
        if [ "$written_pid" = "$$" ]; then
            printf '%s\n' "$old_pid" > "$OTA_PID_FILE" 2>/dev/null
        fi
        log "OTA daemon already running (pid $old_pid)."
        return 1
    fi
    rm -rf "$DAEMON_LOCK" 2>/dev/null
    if mkdir "$DAEMON_LOCK" 2>/dev/null; then
        printf '%s\n' "$$" > "$DAEMON_LOCK/pid"
        printf '%s\n' "$$" > "$OTA_PID_FILE" 2>/dev/null
        return 0
    fi
    return 1
}

do_daemon() {
    case "$OTA_INTERVAL" in
        ''|*[!0-9]*) OTA_INTERVAL=600 ;;
    esac
    [ "$OTA_INTERVAL" -ge 60 ] || OTA_INTERVAL=60
    case "$OTA_RETRY_INTERVAL" in
        ''|*[!0-9]*) OTA_RETRY_INTERVAL=60 ;;
    esac
    [ "$OTA_RETRY_INTERVAL" -ge 10 ] || OTA_RETRY_INTERVAL=10

    acquire_daemon_lock || return 0

    self="$0"
    case "$self" in
        /*) ;;
        *) self="$(pwd)/$self" ;;
    esac

    log "OTA daemon started (interval ${OTA_INTERVAL}s, retry ${OTA_RETRY_INTERVAL}s)"
    trap 'rm -rf "$DAEMON_LOCK" 2>/dev/null; rm -f "$OTA_PID_FILE" 2>/dev/null; log "OTA daemon stopped"; exit 0' HUP INT TERM

    # Give Wi-Fi/DNS a moment after boot before the first attempt.
    sleep 20
    while true; do
        # start-stop-daemon from a later init script may overwrite the PID
        # file with a duplicate that immediately exits; keep it accurate.
        printf '%s\n' "$$" > "$OTA_PID_FILE" 2>/dev/null
        if "$self" check >>"$LOG_FILE" 2>&1; then
            sleep "$OTA_INTERVAL"
        else
            log "OTA check failed; retrying in ${OTA_RETRY_INTERVAL}s."
            sleep "$OTA_RETRY_INTERVAL"
        fi
    done
}

do_status() {
    current_version=$(cat "$CURRENT_VERSION_FILE" 2>/dev/null || printf 'none')
    last_result=$(cat "$LAST_RESULT_FILE" 2>/dev/null || printf 'never')
    last_check=$(cat "$LAST_CHECK_FILE" 2>/dev/null || printf 'never')
    daemon_pid=$(cat "$OTA_PID_FILE" 2>/dev/null || printf 'not running')
    lock_pid=$(cat "$DAEMON_LOCK/pid" 2>/dev/null || printf 'none')
    printf 'daemon:    %s\n' "$daemon_pid"
    printf 'lock:      %s\n' "$lock_pid"
    printf 'installed: %s\n' "$current_version"
    printf 'last:      %s\n' "$last_result"
    printf 'checked:   %s\n' "$last_check"
    printf 'interval:  %ss\n' "$OTA_INTERVAL"
    if [ -f "$PENDING_RESTART" ]; then
        printf 'restart:   pending (quiet hours)\n'
    else
        printf 'restart:   not pending\n'
    fi
    printf 'manifest:  %s\n' "$MANIFEST_URL"
    printf 'backup:    %s\n' "$BACKUP"
    [ -f "$LOG_FILE" ] && tail -n 15 "$LOG_FILE"
    return 0
}

do_rollback() {
    acquire_lock
    latest=$(ls -1d "$BACKUP"/* 2>/dev/null | sort -r | head -n 1)
    [ -n "$latest" ] && [ -d "$latest" ] || die "no backup available"
    log "Rolling back to $(basename "$latest")"
    restore_snapshot "$latest" || die "rollback failed"
    set_result "rolled back to $(basename "$latest")"
    restart_services
    return 0
}

cmd="${1:-check}"
case "$cmd" in
    check)   do_check 0 ;;
    force)   do_check 1 ;;
    status)  do_status ;;
    rollback) do_rollback ;;
    daemon|loop) do_daemon ;;
    *)
        printf 'Usage: %s {check|force|status|rollback|daemon}\n' "$0" >&2
        exit 2
        ;;
esac
