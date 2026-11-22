#!/usr/bin/env python3
"""Generate OTA manifests and publish a GitHub release.

The GitHub token is read only from the GITHUB_TOKEN environment variable.
It is never written to a file, manifest, log line or command line.

Usage:
  GITHUB_TOKEN=... python3 ota/publish_release.py
  python3 ota/publish_release.py --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API = "https://api.github.com"
REPO = "dikverazaza/capsule-alice"
ROOT = Path(__file__).resolve().parent.parent
OTA_DIR = ROOT / "ota"


def api(method: str, path: str, token: str | None, payload=None):
    url = API + path
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "capsula-ota-publisher",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"GitHub API {method} {path} failed: HTTP {exc.code}: {detail}") from exc


def upload_asset(release_id: int, name: str, source: Path, token: str):
    url = (
        f"https://uploads.github.com/repos/{REPO}/releases/{release_id}/assets"
        f"?name={urllib.parse.quote(name)}"
    )
    data = source.read_bytes()
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(data)),
        "User-Agent": "capsula-ota-publisher",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"asset upload {name} failed: HTTP {exc.code}: {detail}") from exc


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(cfg: dict):
    tag = cfg["tag"]
    version = cfg["version"]
    base = cfg.get(
        "asset_url_base",
        f"https://raw.githubusercontent.com/{REPO}/main/ota/assets",
    ).rstrip("/")

    lines = ["V|1", f"VERSION|{version}"]
    json_files = []
    for item in cfg["files"]:
        source = (ROOT / item["source"]).resolve()
        if not source.is_file():
            raise FileNotFoundError(f"missing OTA asset source: {source}")
        digest = sha256_file(source)
        size = source.stat().st_size
        asset = item["asset"]
        url = f"{base}/{asset}"
        lines.append(
            "|".join(
                [
                    "FILE",
                    item["dest"],
                    asset,
                    url,
                    digest,
                    str(size),
                    str(item.get("mode", "755")),
                ]
            )
        )
        json_files.append(
            {
                "dest": item["dest"],
                "asset": asset,
                "url": url,
                "sha256": digest,
                "size": size,
                "mode": str(item.get("mode", "755")),
            }
        )

    manifest_txt = "\n".join(lines) + "\n"
    manifest_json = json.dumps(
        {"version": version, "tag": tag, "files": json_files},
        ensure_ascii=False,
        indent=2,
    ) + "\n"
    return manifest_txt, manifest_json, json_files


def ensure_release(tag: str, cfg: dict, token: str):
    payload = {
        "tag_name": tag,
        "name": cfg.get("name", tag),
        "body": cfg.get("body", ""),
        "draft": False,
        "prerelease": False,
    }
    try:
        return api("POST", f"/repos/{REPO}/releases", token, payload)
    except RuntimeError as exc:
        if "HTTP 422" not in str(exc):
            raise
        # Release already exists for this tag; reuse it.
        return api("GET", f"/repos/{REPO}/releases/tags/{urllib.parse.quote(tag)}", token)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(OTA_DIR / "release.json"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    manifest_txt, manifest_json, files = build_manifest(cfg)

    (OTA_DIR / "manifest.txt").write_text(manifest_txt, encoding="utf-8", newline="\n")
    (OTA_DIR / "manifest.json").write_text(manifest_json, encoding="utf-8", newline="\n")

    print(f"manifest {cfg['version']}: {len(files)} files")
    for item in files:
        print(f"  {item['asset']:<28} {item['size']:>9} {item['sha256'][:12]}")

    if args.dry_run:
        print("dry run: release not published")
        return 0

    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        print("GITHUB_TOKEN is not set", file=sys.stderr)
        return 2

    release = ensure_release(cfg["tag"], cfg, token)
    release_id = int(release["id"])

    existing = api("GET", f"/repos/{REPO}/releases/{release_id}/assets", token)
    by_name = {a["name"]: a["id"] for a in existing}
    for item in files:
        if item["asset"] in by_name:
            api("DELETE", f"/repos/{REPO}/releases/assets/{by_name[item['asset']]}", token)

    sources_by_asset = {item["asset"]: item["source"] for item in cfg["files"]}
    for item in files:
        source = (ROOT / sources_by_asset[item["asset"]]).resolve()
        upload_asset(release_id, item["asset"], source, token)
        print(f"uploaded {item['asset']}")

    print(f"release: {release.get('html_url', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
