#!/usr/bin/env bash
# End-to-end: build -> unit tests -> cluster -> benchmarks -> regression gate -> chaos -> reports.
set -euo pipefail
cd "$(dirname "$0")/.."

CLI=./kafkacore-cli/build/install/kafkacore/bin/kafkacore

echo "==> Build + unit tests"
./gradlew build :kafkacore-cli:installDist --console=plain

echo "==> Start 3-node KRaft cluster"
$CLI cluster up
$CLI cluster status

echo "==> Benchmarks"
$CLI bench run workloads/smoke.yaml -o results/smoke
$CLI bench run workloads/baseline.yaml -o results/baseline \
  --baseline benchmarks/baselines/baseline.json --policy workloads/regression-policy.yaml || \
  echo "(regression gate reported FAIL — see results/baseline/summary.md)"
$CLI bench run workloads/message-size-sweep.yaml -o results/size-sweep

echo "==> Fault injection"
$CLI chaos run all -o results/chaos

echo "==> Integration test suite"
./gradlew :kafkacore-it:integrationTest --console=plain

echo
echo "Reports:"
ls -1 results/*/*.html
