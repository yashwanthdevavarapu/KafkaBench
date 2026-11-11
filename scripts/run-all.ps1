# Windows equivalent of scripts/run-all.sh
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$cli = ".\kafkacore-cli\build\install\kafkacore\bin\kafkacore.bat"

Write-Host "==> Build + unit tests"
.\gradlew.bat build :kafkacore-cli:installDist --console=plain

Write-Host "==> Start 3-node KRaft cluster"
& $cli cluster up
& $cli cluster status

Write-Host "==> Benchmarks"
& $cli bench run workloads/smoke.yaml -o results/smoke
& $cli bench run workloads/baseline.yaml -o results/baseline --baseline benchmarks/baselines/baseline.json --policy workloads/regression-policy.yaml
& $cli bench run workloads/message-size-sweep.yaml -o results/size-sweep

Write-Host "==> Fault injection"
& $cli chaos run all -o results/chaos

Write-Host "==> Integration test suite"
.\gradlew.bat :kafkacore-it:integrationTest --console=plain
