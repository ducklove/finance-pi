#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${FINANCE_PI_ROOT:-$HOME/Works/finance-pi}"
BACKUP_DIR="${FINANCE_PI_BACKUP_DIR:-$HOME/backups/finance-pi}"
MODE="${1:-create}"
ARCHIVE="${2:-}"
LOCK_FILE="$APP_DIR/data/_state/pipeline.lock"

mkdir -p "$BACKUP_DIR" "$(dirname "$LOCK_FILE")"

latest_archive() {
  find "$BACKUP_DIR" -maxdepth 1 -type f -name 'finance-pi-source-*.tar.zst' \
    -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f2-
}

verify_archive() {
  local archive="$1"
  local checksum="$archive.sha256"
  [ -f "$archive" ] || { echo "backup archive not found: $archive" >&2; return 1; }
  [ -f "$checksum" ] || { echo "backup checksum not found: $checksum" >&2; return 1; }
  (cd "$(dirname "$archive")" && sha256sum -c "$(basename "$checksum")")
  local drill_dir
  drill_dir=$(mktemp -d "$BACKUP_DIR/.restore-drill.XXXXXX")
  trap 'rm -rf "$drill_dir"' RETURN
  tar --zstd -xf "$archive" -C "$drill_dir"
  [ -d "$drill_dir/data/bronze" ]
  [ -d "$drill_dir/data/_state" ]
  [ -f "$drill_dir/BACKUP-METADATA.txt" ]
  local files
  files=$(find "$drill_dir" -type f | wc -l)
  echo "restore drill: OK ($files files extracted)"
  rm -rf "$drill_dir"
  trap - RETURN
}

case "$MODE" in
  create)
    exec 9>"$LOCK_FILE"
    flock --wait 600 9
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    archive="$BACKUP_DIR/finance-pi-source-$stamp.tar.zst"
    staging=$(mktemp -d "$BACKUP_DIR/.backup-stage.XXXXXX")
    trap 'rm -rf "$staging" "${archive:-}.partial"' EXIT
    git_head=$(git -C "$APP_DIR" rev-parse HEAD)
    {
      echo "created_utc=$stamp"
      echo "git_head=$git_head"
      echo "scope=reconstructable source lake, macro data, ingest state/cache, and runtime config"
    } >"$staging/BACKUP-METADATA.txt"
    source_items=(pyproject.toml data/bronze data/macro data/_state data/_cache)
    if [ -f "$APP_DIR/.env" ]; then
      source_items=(.env "${source_items[@]}")
    fi
    tar --zstd -cf "$archive.partial" -C "$APP_DIR" \
      --transform='s#^\.env$#runtime/.env#' \
      "${source_items[@]}" \
      -C "$staging" BACKUP-METADATA.txt
    mv "$archive.partial" "$archive"
    chmod 600 "$archive"
    (cd "$BACKUP_DIR" && sha256sum "$(basename "$archive")" >"$(basename "$archive").sha256")
    chmod 600 "$archive.sha256"
    verify_archive "$archive"
    find "$BACKUP_DIR" -maxdepth 1 -type f \( -name 'finance-pi-source-*.tar.zst' -o -name 'finance-pi-source-*.tar.zst.sha256' \) -mtime +14 -delete
    echo "backup created: $archive"
    ;;
  verify)
    if [ -z "$ARCHIVE" ]; then
      ARCHIVE=$(latest_archive)
    fi
    [ -n "$ARCHIVE" ] || { echo "no backup archive found" >&2; exit 1; }
    verify_archive "$ARCHIVE"
    ;;
  *)
    echo "usage: $0 [create|verify] [archive]" >&2
    exit 2
    ;;
esac
