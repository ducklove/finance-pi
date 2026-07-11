#!/usr/bin/env bash
# finance-pi deployment + verification. Runs ON the server (Raspberry Pi).
#
# Usage:
#   bash ops/deploy.sh [--full-rebuild] [--pit] [--skip-tests] [--no-restart]
#
# Options:
#   --full-rebuild  Kick off the one-time full silver/gold rebuild in the
#                   background (required once after the price_basis /
#                   corporate-actions deployment; can take hours).
#   --pit           Also rebuild gold.fundamentals_pit in the background
#                   (first run is a full build, later runs are incremental).
#   --skip-tests    Skip the pytest gate (not recommended).
#   --no-restart    Do not restart the admin systemd service.
#   --stash         Stash uncommitted local changes (with a labeled stash
#                   entry) before pulling instead of aborting.
#
# Environment overrides:
#   FINANCE_PI_ROOT   repo path      (default: $HOME/Works/finance-pi)
#   FINANCE_PI_BRANCH branch         (default: main)
#   FINANCE_PI_ADMIN_URL admin URL   (default: http://127.0.0.1:8400)
set -euo pipefail

FINANCE_PI_ROOT="${FINANCE_PI_ROOT:-$HOME/Works/finance-pi}"
BRANCH="${FINANCE_PI_BRANCH:-main}"
ADMIN_URL="${FINANCE_PI_ADMIN_URL:-http://127.0.0.1:8400}"
PIPELINE_LOCK="$FINANCE_PI_ROOT/data/_state/pipeline.lock"

FULL_REBUILD=0
PIT=0
SKIP_TESTS=0
NO_RESTART=0
STASH=0
for arg in "$@"; do
  case "$arg" in
    --full-rebuild) FULL_REBUILD=1 ;;
    --pit) PIT=1 ;;
    --skip-tests) SKIP_TESTS=1 ;;
    --no-restart) NO_RESTART=1 ;;
    --stash) STASH=1 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

log() { printf '\n[deploy] %s\n' "$*"; }
fail() { printf '\n[deploy] ERROR: %s\n' "$*" >&2; exit 1; }

[ -d "$FINANCE_PI_ROOT/.git" ] || fail "repo not found at $FINANCE_PI_ROOT (set FINANCE_PI_ROOT)"
cd "$FINANCE_PI_ROOT"

if [ -f .env ]; then
  chmod 600 .env
fi

VENV="$FINANCE_PI_ROOT/.venv"
PY="$VENV/bin/python"
[ -x "$PY" ] || fail "venv not found at $VENV (see docs/raspberry-pi.md section 2)"

log "1/7 update code ($BRANCH)"
if ! git diff --quiet || ! git diff --cached --quiet; then
  if [ "$STASH" -eq 1 ]; then
    STASH_LABEL="deploy-autostash-$(date +%Y%m%d-%H%M%S)"
    git stash push -m "$STASH_LABEL"
    echo "  local changes stashed as '$STASH_LABEL' (restore: git stash pop)"
  else
    git status --short
    fail "uncommitted local changes on the server. Commit them to a branch, or rerun with --stash to set them aside."
  fi
fi
git fetch origin "$BRANCH"
git checkout -q "$BRANCH"
BEFORE=$(git rev-parse --short HEAD)
git pull --ff-only origin "$BRANCH"
AFTER=$(git rev-parse --short HEAD)
echo "  $BEFORE -> $AFTER"

log "2/7 install dependencies"
"$PY" -m pip install -q -e ".[dev]"

log "3/7 configuration check (doctor)"
"$PY" -m finance_pi.cli.app doctor --root "$FINANCE_PI_ROOT" \
  || echo "  WARNING: doctor reported issues; review the output above"

if [ "$SKIP_TESTS" -eq 1 ]; then
  log "4/7 tests skipped (--skip-tests)"
else
  log "4/7 test suite"
  "$PY" -m pytest -q
fi

log "5/7 restart services"
USER_UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$USER_UNIT_DIR"
for unit_file in "$FINANCE_PI_ROOT"/ops/systemd/*.{service,timer}; do
  [ -f "$unit_file" ] || continue
  install -m 0644 "$unit_file" "$USER_UNIT_DIR/$(basename "$unit_file")"
done
systemctl --user daemon-reload
systemctl --user enable --now finance-pi-backup.timer >/dev/null

APACHECTL=$(command -v apache2ctl 2>/dev/null || true)
if [ -z "$APACHECTL" ] && [ -x /usr/sbin/apache2ctl ]; then
  APACHECTL=/usr/sbin/apache2ctl
fi
if [ -n "$APACHECTL" ] && sudo -n true 2>/dev/null; then
  sudo -n install -m 0644 \
    "$FINANCE_PI_ROOT/ops/apache/finance-pi-admin.conf" \
    /etc/apache2/conf-available/finance-pi-admin.conf
  if sudo -n "$APACHECTL" configtest >/dev/null 2>&1; then
    sudo -n systemctl reload apache2
    echo "  updated Apache finance-pi proxy configuration"
  else
    echo "  WARNING: Apache configuration test failed; proxy was not reloaded"
  fi
fi

restart_unit() {
  local unit="$1"
  if systemctl --user list-unit-files --no-legend "$unit" 2>/dev/null | grep -q "^$unit"; then
    if systemctl --user restart "$unit" 2>/dev/null; then
      echo "  restarted $unit (user unit)"
    else
      echo "  WARNING: could not restart user unit $unit."
      echo "           run manually: systemctl --user restart $unit"
    fi
  elif systemctl list-unit-files --no-legend "$unit" 2>/dev/null | grep -q "^$unit"; then
    if sudo -n systemctl restart "$unit" 2>/dev/null; then
      echo "  restarted $unit"
    else
      echo "  WARNING: cannot restart $unit without a sudo password."
      echo "           run manually: sudo systemctl restart $unit"
    fi
  else
    echo "  $unit not installed; skipping"
  fi
}
if [ "$NO_RESTART" -eq 1 ]; then
  echo "  restart skipped (--no-restart)"
else
  # Timer-driven jobs (daily/catchup) start fresh processes; only the
  # long-lived admin server needs a restart to pick up new code.
  restart_unit finance-pi-admin.service
fi

log "6/7 runtime verification"
sleep 2
if curl -fsS --max-time 15 "$ADMIN_URL/api/ready" >/dev/null 2>&1; then
  echo "  admin readiness: OK"
  "$PY" -m finance_pi.cli.app check-admin "$ADMIN_URL"
else
  fail "admin is live but not ready at $ADMIN_URL/api/ready"
fi
FACTOR_COUNT=$("$PY" -m finance_pi.cli.app factors list | wc -l | tr -d ' ')
echo "  registered factors: $FACTOR_COUNT (expected 13 or more)"
"$PY" -c "from finance_pi.mcp_server import tools; print('  mcp_server logic layer import: OK')"

log "7/7 post-deploy data migration"
mkdir -p "$FINANCE_PI_ROOT/logs" "$(dirname "$PIPELINE_LOCK")"
STAMP=$(date +%Y%m%d-%H%M%S)
if [ "$FULL_REBUILD" -eq 1 ]; then
  REBUILD_LOG="$FINANCE_PI_ROOT/logs/deploy-rebuild-$STAMP.log"
  echo "  starting full silver/gold rebuild in the background (hours on a Pi)"
  nohup /usr/bin/flock --nonblock "$PIPELINE_LOCK" bash -c \
    "'$PY' -m finance_pi.cli.app build all --root '$FINANCE_PI_ROOT' \
       && '$PY' -m finance_pi.cli.app catalog build --root '$FINANCE_PI_ROOT'" \
    >>"$REBUILD_LOG" 2>&1 &
  echo "  rebuild PID $!  |  follow with: tail -f $REBUILD_LOG"
else
  echo "  NOTE: this release changed price adjustment semantics (price_basis,"
  echo "        corporate actions, delisting detection). Run the one-time full"
  echo "        rebuild when convenient:  bash ops/deploy.sh --full-rebuild"
fi
if [ "$PIT" -eq 1 ]; then
  PIT_LOG="$FINANCE_PI_ROOT/logs/deploy-pit-$STAMP.log"
  echo "  starting fundamentals_pit rebuild in the background"
  nohup /usr/bin/flock --nonblock "$PIPELINE_LOCK" \
    "$PY" -m finance_pi.cli.app build fundamentals-pit --root "$FINANCE_PI_ROOT" \
    >>"$PIT_LOG" 2>&1 &
  echo "  pit PID $!  |  follow with: tail -f $PIT_LOG"
fi

log "deployment complete at $AFTER"
