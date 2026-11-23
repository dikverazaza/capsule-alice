# OTA updates for VK Capsule Mini

Silent, checksum-verified updates from public GitHub raw files. No token is stored on the capsule.

The capsule's BusyBox `wget` can reach `raw.githubusercontent.com` but cannot complete TLS to `api.github.com` or `github.com`, so both manifest and binaries are served from raw GitHub content on the `main` branch. A moving tag such as `ota-latest` can keep resolving to an old commit in the raw CDN for hours; `main` becomes current immediately after publishing.

## How it works

1. `/usr/bin/capsula-ota check` downloads `https://raw.githubusercontent.com/dikverazaza/capsule-alice/main/ota/manifest.txt`.
2. The manifest lists version, destination path, raw asset URL, SHA-256, size and mode for each file.
3. Files whose installed SHA-256 already matches the manifest are skipped. Changed assets are downloaded to `/tmp/capsula-ota-work` (tmpfs), then size and SHA-256 are checked **before** any installed file is touched.
4. Existing files are copied to `/data/ota/backup/<old-version>`. When `/data` has less than 12 MB free, snapshots move to `/tmp/capsula-ota-backup`.
5. New files are written as `<dest>.new`, `chmod`-ed, `sync`-ed and atomically `mv`-ed into place.
6. On any install failure, the backup is restored automatically.
7. If service binaries changed, the Alice launcher is restarted quietly. It does **not** unmute the speaker and caps volume at 20%.
8. Restart is applied in the same OTA run. The client re-reads the config after install, so a new `DEFER_RESTART_AT_NIGHT=0` takes effect immediately. Quiet hours are disabled by default; set `DEFER_RESTART_AT_NIGHT=1` only if restart deferral is explicitly needed.

The updater only accepts a fixed whitelist of destinations:

```text
/usr/bin/capsula-alice
/usr/bin/capsula-audio
/usr/bin/capsula-yandex
/usr/bin/capsula-hub
/usr/bin/capsula-ota
/usr/bin/capsula-alice-launcher.sh
/usr/bin/wakewordd
/usr/bin/ring_daemon
/usr/bin/capsule_onboarding
/etc/init.d/S98capsula-ota
/etc/init.d/S50avahi-daemon
/etc/capsula-ota.conf
```

Asset URLs must point to `https://raw.githubusercontent.com/dikverazaza/capsule-alice/main/ota/assets/...`.

Version comparison uses `sort -V`; an older manifest is ignored so a stale raw CDN response cannot downgrade a capsule. Recovery from a newer version back to an older one is possible only with `capsula-ota force`.

## Device commands

```sh
capsula-ota check        # default: download, verify, install if version differs
capsula-ota force        # reinstall even if version matches
capsula-ota status       # installed version, last check, last result, recent log lines
capsula-ota rollback     # restore newest backup
capsula-ota daemon       # check every 600 s, standalone, independent of Alice
```

Logs: `/tmp/capsula-ota.log` and persistent `/data/ota/ota.log`. State: `/data/ota/` (`current_version`, `last_result`, `last_check`, `restart_pending`, `backup/`). Each check runs as a separate child process, so a failed check cannot kill the daemon.

Optional local configuration in `/etc/capsula-ota.conf`:

```sh
ENABLED=1
RESTART_SERVICES=1
DEFER_RESTART_AT_NIGHT=0
QUIET_START=23
QUIET_END=7
OTA_INTERVAL=600
OTA_RETRY_INTERVAL=60
# OTA_HOME=/data/ota
# CAPSULA_OTA_BACKUP_HOME=/data/ota/backup
# CAPSULA_OTA_BASE_URL=https://example.com/ota
# CAPSULA_OTA_MANIFEST_URL=https://example.com/custom/manifest.txt
# CAPSULA_OTA_ASSET_PREFIX=https://example.com/ota/assets/
```

`/etc/init.d/S98capsula-ota start` launches the standalone daemon with `start-stop-daemon`. It is also started from `S50avahi-daemon`, which is definitely executed by `rcS` on this firmware. A daemon-owned lock directory `/data/ota/.daemon.lock` prevents duplicates even if both init scripts race at boot. The daemon waits 20 s for Wi-Fi/DNS, retries a failed check after 60 s instead of 10 minutes, then returns to the 600 s interval. It does not use the Alice launcher, PulseAudio, or any other service, so updates still arrive after a reboot or if Alice is dead.

## First-time bootstrap

Copy the updater once over ADB/SSH, then let OTA install everything else:

```sh
adb push ota/capsula-ota.sh /usr/bin/capsula-ota
adb shell chmod 755 /usr/bin/capsula-ota
adb push ota/S98capsula-ota /etc/init.d/S98capsula-ota
adb shell chmod 755 /etc/init.d/S98capsula-ota
adb shell /usr/bin/capsula-ota check
adb shell /usr/bin/capsula-ota status
```

After that, updates are delivered by the release manifest itself, including updater and init-script self-updates.

## Publishing a release

Releases are automatic. `.github/workflows/ota-release.yml` runs on every push to `main` unless the commit message contains `[skip ota]`. It:

1. installs Rust `aarch64-unknown-linux-musl` and Zig 0.17.0,
2. rebuilds `capsula-alice` from source and copies it into `ota/assets/`,
3. increments the patch number in `ota/release.json` (`20261123.17` → `.18`),
4. runs `publish_release.py` with the Actions-provided `GITHUB_TOKEN`,
5. commits the regenerated manifest, release metadata and binary back to `main` with a `[skip ota]` message so the workflow does not loop.

No token is stored on a capsule or in the repository. `wakewordd` is a prebuilt TFLite binary committed under `ota/assets/`; update that file in the repo when the spotter changes.

For a manual one-off release outside Actions:

```sh
python3 ota/publish_release.py --dry-run
GITHUB_TOKEN=... python3 ota/publish_release.py
```

`publish_release.py` uses only Python's standard library. It writes `ota/manifest.txt` plus `ota/manifest.json`, and mirrors the assets into a GitHub Release for archival. The `ota-latest` tag may still be moved for archival convenience, but it is no longer used by the client because the raw CDN caches moved tags too aggressively.

The earlier ETXTBSY rollback is handled as well: if `mv` into a running executable fails, the updater stops that service and retries up to five times before considering the install failed.

## Security notes

- BusyBox `wget` on the capsule does **not** validate TLS certificates. The manifest therefore cannot be authenticated by TLS alone.
- SHA-256 + size verification protects assets from corruption and from a modified manifest only as far as the manifest itself is trusted. The manifest is served from the project's public GitHub repository.
- Destinations are whitelisted and release URLs are restricted, so even a tampered manifest cannot be used to overwrite arbitrary paths or fetch from arbitrary hosts.
- For higher assurance, pin `CAPSULA_OTA_MANIFEST_URL` to a trusted local endpoint or replace the manifest with a signed one and add verification before deployment.
