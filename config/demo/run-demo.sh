#!/bin/sh
# =============================================================================
# cesium-kafka self-driving demo (run via `make demo` or the compose `demo` profile).
#
# Submits five "notifications" to the source topic ALL AT ONCE, in an order that
# is deliberately NOT their delivery order, each asking to be delivered after a
# different delay (one asks for immediate delivery). It then watches the
# destination topic and prints each notification as it ARRIVES — so you can see
# cesium re-order them by requested time and deliver each one on schedule,
# exactly once, with the control header stripped.
#
# Needs nothing on your machine but Docker: kcat runs in this sidecar container.
# =============================================================================
set -eu

BROKER="${BROKER:-kafka:29092}"
SRC="${SRC:-orders}"
DST="${DST:-orders-delayed}"
READY_URL="${READY_URL:-http://cesium:8081/health/ready}"

# ---- 1. Wait until cesium is ready (best-effort; topic-init already seeded offsets) ----
printf 'demo: waiting for cesium to be ready'
i=0
while [ "$i" -lt 60 ]; do
  if wget -q -T 2 -O - "$READY_URL" >/dev/null 2>&1; then
    printf ' ... ready.\n'
    break
  fi
  printf '.'
  i=$((i + 1))
  sleep 2
done
[ "$i" -lt 60 ] || printf ' (gave up waiting; producing anyway)\n'

# ---- 2. Submit five notifications at once, in submission order n1..n5 ----
# header `cesium-delay-ms=N` => deliver N ms after submission; no header => immediately.
submit() { # key  label  delay-ms-or-empty
  if [ -n "${3:-}" ]; then
    printf '%s' "$2" | kcat -b "$BROKER" -t "$SRC" -P -k "$1" -H "cesium-delay-ms=$3"
  else
    printf '%s' "$2" | kcat -b "$BROKER" -t "$SRC" -P -k "$1"
  fi
}

submit_s=$(date +%s)   # baseline for the "+Ns" arrival labels below
echo "demo: submitting 5 notifications to '$SRC' at $(date +%H:%M:%S) — all at once:"
submit n1 'Welcome email'   20000; echo '  submitted n1  "Welcome email"     -> deliver in 20s'
submit n2 'Reminder ping'    5000; echo '  submitted n2  "Reminder ping"      -> deliver in  5s'
submit n3 'Shipping notice' 15000; echo '  submitted n3  "Shipping notice"    -> deliver in 15s'
submit n4 'Wake-up call'    10000; echo '  submitted n4  "Wake-up call"       -> deliver in 10s'
submit n5 'Receipt'              ; echo '  submitted n5  "Receipt"            -> deliver immediately'

echo ''
echo "demo: watching '$DST' (isolation.level=read_committed). Expected ARRIVAL order:"
echo "      Receipt (now), Reminder (+5s), Wake-up (+10s), Shipping (+15s), Welcome (+20s)."
echo "      Note: submission order was n1,n2,n3,n4,n5 — delivery is re-ordered by requested time."
echo ''

# ---- 3. Consume the destination; label each arrival with "+Ns" = its delivery offset ----
# from submission, computed from the message's OWN dispatch timestamp (%T, the wall-clock
# when cesium relayed it) — so it is accurate regardless of how the consumer buffers output.
# -c 5 exits after the five arrive (a fresh stack; `make demo` resets one first).
kcat -b "$BROKER" -t "$DST" -C -o beginning -c 5 -X isolation.level=read_committed -f '%T\t%k\t%s\n' \
  | while IFS="$(printf '\t')" read -r tms key payload; do
      es=$((tms / 1000 - submit_s))
      printf '  ARRIVED  +%2ss  key=%-3s payload="%s"\n' "$es" "$key" "$payload"
    done

echo ''
echo "demo: done — five notifications delivered on schedule, re-ordered, exactly once."
