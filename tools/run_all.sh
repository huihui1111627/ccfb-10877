#!/usr/bin/env bash
# One-command end-to-end reproduction: generate logs, run the platform,
# and produce every quantitative report under data/.
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/logscope
mkdir -p data

echo "==> [1/5] building release binary"
cargo build --release

echo "==> [2/5] generating synthetic logs (5 services, 5 injected faults)"
python3 -I tools/gen_logs.py data/logs

echo "==> [3/5] replaying through the engine (fresh template library)"
rm -rf data/state
$BIN --state-dir data/state bench data/logs/all.log --fresh \
  --report data/bench.json \
  --assignments data/assignments.jsonl \
  --events-out data/events.jsonl

echo "==> [4/5] evaluating accuracy, detection latency and false positives"
python3 -I tools/evaluate.py \
  data/logs/truth.jsonl data/logs/faults.json \
  data/assignments.jsonl data/events.jsonl \
  data/bench.json data/eval-report.json

echo "==> [5/5] large-scale throughput run (~3.4M lines, SCALE=10)"
SCALE=10 python3 -I tools/gen_logs.py data/logs-big
rm -rf data/state-big
$BIN --state-dir data/state-big bench data/logs-big/all.log --fresh \
  --report data/bench-big.json

echo
echo "Done. Key reports:"
echo "  data/bench.json         throughput + template/event counts"
echo "  data/eval-report.json   clustering F1, fault recall/latency, FP rate"
echo "  data/bench-big.json     large-scale throughput"
echo
echo "Inspect templates:   $BIN --state-dir data/state inspect"
echo "Run the web UI:       $BIN --state-dir data/state serve"
