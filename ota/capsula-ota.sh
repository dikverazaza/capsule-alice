#!/bin/sh
# /usr/bin/capsula-ota
# Silent OTA updater for VK Capsule Mini (Buildroot, BusyBox tools only).
#
# Commands:
#   check     download manifest, verify, install if version differs (default)
#   force     same as check but ignores the locally installed version
#   status    print current/manifest version and last result
#   rollback  restore the newest local backup
#
# No token is stored on the device. Assets are public GitHub release files.
# BusyBox wget does not validate TLS certificates, so every asset is verified
# against SHA-256 + size values from the manifest before any file is replaced.

set -u

OTA_BASE_URL="${CAPSULA_OTA_BASE_URL:-https://raw.githubusercontent.com/dikverazaza/capsule-alice/main/ota}"
MANIFEST_URL="${CAPSULA_OTA_MANIFEST_URL:-$OTA_BASE_URL/manifest.txt}"
RELEASE_PREFIX="${CAPSULA_OTA_RELEASE_PREFIX:-https://github.com/dikverazaza/capsule-alice/releases/download/}"

OTA_HOME="${CAPSULA_OTA_HOME:-/data/ota}"
WORK="$OTA_HOME/work"
BACKUP="$OTA_HOME/backup"
CONF="${CAPSULA_OTA_CONF:-/etc/capsula-ota.conf}"
LOG_FILE="/tmp/capsula-ota.log"

# Safe defaults; /etc/capsula-ota.conf may override them.
ENABLED=1
RESTART_SERVICES=1
DEFER_RESTART_AT_NIGHT=1
QUIET_START=23
QUIET_END=7

[ -f "$CONF" ] && . "$CONF"

CURRENT_VERSION_FILE="$OTA_HOME/current_version"
LAST_RESULT_FILE="$OTA_HOME/last_result"
PENDING_RESTART="$OTA_HOME/restart_pending"
LOCK_DIR="$OTA_HOME/.lock"

mkdir -p "$OTA_HOME" "$WORK" "$BACKUP" || exit 1

rotate_log() {
    [ -f "$LOG_FILE" ] || return 0
    size=$(wc -c < "$LOG_FILE" | tr -d ' ')
    if [ "${size:-0}" -gt 200000 ]; then
        tail -n 300 "$LOG_FILE" > "$LOG_FILE.rot" 2>/dev/null
        mv -f "$LOG_FILE.rot" "$LOG_FILE" 2>/dev/null
    fi
}

log() {
    ts=$(date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || printf 'unknown-time')
    printf '%s %s\n' "$ts" "$*" >> "$LOG_FILE" 2>/dev/null
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
    trap 'rm -rf "$LOCK_DIR" 2>/dev/null' EXIT HUP INT TERM
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
        /etc/init.d/S98capsula-ota)
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
        if wget -q -T 30 -O "$_out.new" "$_url" 2>>"$LOG_FILE"; then
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
    sha256sum "$1" 2>/dev/null | awk '{print $1}'
}

size_of() {
    set -- $(wc -c < "$1" 2>/dev/null)
    printf '%s\n' "${1:-0}"
}

is_quiet_hour() {
    [ "$DEFER_RESTART_AT_NIGHT" = "1" ] || return 1
    hour=$(date +%H 2>/dev/null | sed 's/^0//')
    [ -n "$hour" ] || return 1
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

flush_pending_restart() {
    [ -f "$PENDING_RESTART" ] || return 0
    if is_quiet_hour; then
        log "Pending restart still deferred during quiet hours."
        return 0
    fi
    log "Applying deferred service restart."
    restart_services
}

restore_snapshot() {
    _snap="$1"
    [ -f "$_snap/files.list" ] || return 1
    while IFS='|' read -r _dest _mode; do
        [ -n "$_dest" ] || continue
        _name=$(basename "$_dest")
        [ -f "$_snap/$_name" ] || continue
        cp -f "$_snap/$_name" "$_dest.new" 2>/dev/null || continue
        chmod "${_mode:-755}" "$_dest.new" 2>/dev/null
        mv -f "$_dest.new" "$_dest" 2>/dev/null || continue
    done < "$_snap/files.list"
    sync
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

    new_version=$(awk -F'|' '$1 == "VERSION" { gsub(/\r/, "", $2); print $2; exit }' "$MANIFEST_FILE")
    case "$new_version" in
        ''|*[!A-Za-z0-9._-]*) die "invalid version in manifest" ;;
    esac

    awk -F'|' '$1 == "FILE" { print $2 "|" $3 "|" $4 "|" $5 "|" $6 "|" $7 }' \
        "$MANIFEST_FILE" | tr -d '\r' > "$_raw"

    [ -s "$_raw" ] || die "manifest has no FILE entries"

    while IFS='|' read -r dest asset url sha size mode; do
        allowed_dest "$dest" || die "destination not whitelisted: $dest"
        case "$asset" in
            ''|*[!A-Za-z0-9._-]*) die "invalid asset name: $asset" ;;
        esac
        case "$url" in
            "$RELEASE_PREFIX"*) ;;
            *) die "asset URL is not an allowed GitHub release URL: $url" ;;
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

do_check() {
    force="$1"
    [ "$ENABLED" = "1" ] || { log "OTA disabled in configuration."; return 0; }

    acquire_lock

    MANIFEST_FILE="$WORK/manifest.txt"
    download_to "$MANIFEST_URL" "$MANIFEST_FILE" || die "cannot download manifest from $MANIFEST_URL"
    log "Manifest downloaded."

    parse_manifest

    flush_pending_restart

    current_version=$(cat "$CURRENT_VERSION_FILE" 2>/dev/null || printf 'none')
    if [ "$force" != "1" ] && [ "$current_version" = "$new_version" ]; then
        log "Version $new_version already installed. Nothing to do."
        set_result "current $new_version"
        return 0
    fi
    log "Update available: $current_version -> $new_version"

    # Download and verify everything before touching any installed file.
    while IFS='|' read -r dest asset url sha size mode; do
        staged="$WORK/$asset"
        download_to "$url" "$staged" || die "download failed: $asset"
        actual_size=$(size_of "$staged")
        [ "$actual_size" = "$size" ] || die "size mismatch for $asset: $actual_size != $size"
        actual_sha=$(sha_of "$staged")
        [ "$actual_sha" = "$sha" ] || die "SHA-256 mismatch for $asset"
        log "Verified $asset ($actual_size bytes)."
    done < "$WORK/files.list"

    # Snapshot the files that are about to be replaced.
    snapshot="$BACKUP/$current_version"
    mkdir -p "$snapshot"
    : > "$snapshot/files.list"
    need_restart=0
    while IFS='|' read -r dest asset url sha size mode; do
        if [ -e "$dest" ]; then
            cp -p "$dest" "$snapshot/$(basename "$dest")" 2>/dev/null || die "backup failed: $dest"
        fi
        printf '%s|%s\n' "$dest" "$mode" >> "$snapshot/files.list"
        case "$dest" in
            /usr/bin/capsula-ota|/etc/init.d/S98capsula-ota) ;;
            *) need_restart=1 ;;
        esac
    done < "$WORK/files.list"

    installed_ok=1
    while IFS='|' read -r dest asset url sha size mode; do
        staged="$WORK/$asset"
        cp -f "$staged" "$dest.new" 2>/dev/null || { installed_ok=0; break; }
        chmod "$mode" "$dest.new" 2>/dev/null || { installed_ok=0; break; }
        sync
        mv -f "$dest.new" "$dest" 2>/dev/null || { installed_ok=0; break; }
        log "Installed $dest"
    done < "$WORK/files.list"

    if [ "$installed_ok" != "1" ]; then
        log "Installation failed; rolling back to $current_version."
        restore_snapshot "$snapshot"
        set_result "rollback to $current_version"
        exit 1
    fi

    sync
    printf '%s\n' "$new_version" > "$CURRENT_VERSION_FILE"
    set_result "updated $new_version"
    log "OTA update completed: $new_version"

    if [ "$need_restart" = "1" ]; then
        restart_services
    fi
    return 0
}

do_status() {
    current_version=$(cat "$CURRENT_VERSION_FILE" 2>/dev/null || printf 'none')
    last_result=$(cat "$LAST_RESULT_FILE" 2>/dev/null || printf 'never')
    printf 'installed: %s\n' "$current_version"
    printf 'last:     %s\n' "$last_result"
    if [ -f "$PENDING_RESTART" ]; then
        printf 'restart:  pending (quiet hours)\n'
    else
        printf 'restart:  not pending\n'
    fi
    printf 'manifest: %s\n' "$MANIFEST_URL"
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
    *)
        printf 'Usage: %s {check|force|status|rollback}\n' "$0" >&2
        exit 2
        ;;
esac
