# OTA updates for VK Capsule Mini

Silent, checksum-verified updates over GitHub Releases. No token is stored on the capsule.

## How it works

1. `/usr/bin/capsula-ota check` downloads `https://raw.githubusercontent.com/dikverazaza/capsule-alice/main/ota/manifest.txt`.
2. The manifest lists version, destination path, release asset URL, SHA-256, size and mode for each file.
3. Every asset is downloaded to `/data/ota/work`, then size and SHA-256 are checked **before** any installed file is touched.
4. Existing files are copied to `/data/ota/backup/<old-version>`.
5. New files are written as `<dest>.new`, `chmod`-ed, `sync`-ed and atomically `mv`-ed into place.
6. On any install failure, the backup is restored automatically.
7. If service binaries changed, the Alice launcher is restarted quietly. It does **not** unmute the speaker and caps volume at 20%.
8. During quiet hours (23:00–07:00 by default) the restart is deferred to the next check outside that window.

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
```

Asset URLs must point to `https://github.com/dikverazaza/capsule-alice/releases/download/...`.

## Device commands

```sh
capsula-ota check        # default: download, verify, install if version differs
capsula-ota force        # reinstall even if version matches
capsula-ota status       # installed version, last result, recent log lines
capsula-ota rollback     # restore newest backup
```

Logs: `/tmp/capsula-ota.log`. State: `/data/ota/` (`current_version`, `last_result`, `restart_pending`, `work/`, `backup/`).

Optional local configuration in `/etc/capsula-ota.conf`:

```sh
ENABLED=1
RESTART_SERVICES=1
DEFER_RESTART_AT_NIGHT=1
QUIET_START=23
QUIET_END=7
# CAPSULA_OTA_MANIFEST_URL=https://example.com/custom/manifest.txt
```

`/etc/init.d/S98capsula-ota start` runs a background check 25 seconds after boot. It is safe to leave enabled.

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

The token is read only from the environment and never written to a manifest, log or config file:

```sh
# edit ota/release.json: tag, version, notes, file list
python3 ota/publish_release.py --dry-run
GITHUB_TOKEN=... python3 ota/publish_release.py
git add ota/manifest.txt ota/manifest.json ota/release.json
git commit -m "OTA release <version>"
git push
```

`publish_release.py` uses only Python's standard library. It creates the GitHub release if needed, removes stale assets with the same names, uploads all assets, and writes `ota/manifest.txt` plus `ota/manifest.json`.

## Security notes

- BusyBox `wget` on the capsule does **not** validate TLS certificates. The manifest therefore cannot be authenticated by TLS alone.
- SHA-256 + size verification protects assets from corruption and from a modified manifest only as far as the manifest itself is trusted. The manifest is served from the project's public GitHub repository.
- Destinations are whitelisted and release URLs are restricted, so even a tampered manifest cannot be used to overwrite arbitrary paths or fetch from arbitrary hosts.
- For higher assurance, pin `CAPSULA_OTA_MANIFEST_URL` to a trusted local endpoint or replace the manifest with a signed one and add verification before deployment.
