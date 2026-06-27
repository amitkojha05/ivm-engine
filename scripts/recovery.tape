# Install vhs: go install github.com/charmbracelet/vhs@latest
# Run: vhs scripts/recovery.tape

Output demo_recovery.gif
Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 700

Type "# IVM Engine — Crash Recovery Demo" Sleep 500ms Enter
Sleep 1s

Type "# 1. Insert rows into Kafka topic" Sleep 300ms Enter
Type "docker exec ivm-kafka kafka-console-producer --topic orders --bootstrap-server localhost:9092 <<< '{\"op\":\"c\",\"after\":{\"id\":1,\"amount\":500}}'" Enter
Sleep 1s

Type "# 2. Check metrics — rows are flowing" Sleep 300ms Enter
Type "curl -s localhost:8080/metrics | grep rows_processed" Enter
Sleep 1s

Type "# 3. CRASH the engine" Sleep 300ms Enter
Type "pkill -f ivm-api" Enter
Sleep 500ms
Type "echo '[engine killed]'" Enter
Sleep 1s

Type "# 4. Restart — restores from Parquet checkpoint" Sleep 300ms Enter
Type "cargo run -p ivm-api --release &" Enter
Sleep 3s

Type "# 5. Verify state recovered" Sleep 300ms Enter
Type "curl -s localhost:8080/metrics | grep rows_processed" Enter
Sleep 500ms
Type "echo 'Recovery complete — no data loss'" Enter
Sleep 2s
