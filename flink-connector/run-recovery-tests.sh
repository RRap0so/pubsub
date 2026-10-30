#!/usr/bin/env bash
set -euo pipefail

# Run entirely against loopback. No cloud project or credentials are required.
cd "$(dirname "$0")"
task_dir=$(mktemp -d "${TMPDIR:-/tmp}/pubsub-recovery.XXXXXX")
emulator_pid=""
cleanup() {
  if [[ -n "$emulator_pid" ]]; then
    kill "$emulator_pid" 2>/dev/null || true
    wait "$emulator_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ -z "${CHECKPOINT_TEST_EMULATOR:-}" ]]; then
  emulator_bin="${PUBSUB_EMULATOR_BIN:-}"
  if [[ -z "$emulator_bin" ]]; then
    if ! command -v gcloud > /dev/null 2>&1; then
      echo "gcloud is not installed. Install the Pub/Sub emulator or set PUBSUB_EMULATOR_BIN." >&2
      exit 1
    fi
    if ! sdk_root=$(gcloud info --format='value(installation.sdk_root)' 2>/dev/null); then
      echo "Unable to locate the Google Cloud SDK. Set PUBSUB_EMULATOR_BIN explicitly." >&2
      exit 1
    fi
    emulator_bin="$sdk_root/platform/pubsub-emulator/bin/cloud-pubsub-emulator"
  fi
  if [[ ! -x "$emulator_bin" ]]; then
    echo "Install the Pub/Sub emulator or set PUBSUB_EMULATOR_BIN." >&2
    exit 1
  fi
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  export CHECKPOINT_TEST_EMULATOR="127.0.0.1:$port"
  "$emulator_bin" --host=127.0.0.1 --port="$port" > "$task_dir/emulator.log" 2>&1 &
  emulator_pid=$!
fi

if [[ ! "$CHECKPOINT_TEST_EMULATOR" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
  echo "CHECKPOINT_TEST_EMULATOR must be 127.0.0.1:<port>." >&2
  exit 1
fi
export PUBSUB_EMULATOR_HOST="$CHECKPOINT_TEST_EMULATOR"

ready=false
for attempt in {1..100}; do
  if curl --silent --fail --max-time 1 \
      "http://$CHECKPOINT_TEST_EMULATOR/v1/projects/checkpoint-repro/topics" > /dev/null; then
    ready=true
    break
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "Emulator did not become ready. See $task_dir/emulator.log" >&2
  exit 1
fi

echo "Using local emulator: $CHECKPOINT_TEST_EMULATOR"
echo "Emulator log: $task_dir/emulator.log"
mvn -B -pl flink-connector-gcp-pubsub -Precovery-it clean verify "$@"
