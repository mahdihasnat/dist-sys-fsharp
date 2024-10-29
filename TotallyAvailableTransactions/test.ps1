#!/usr/bin/env pwsh

# Exit on error, and clear the console
$ErrorActionPreference = "Stop"
Clear-Host

# Publish the .NET project in Release mode
dotnet publish -c Release

# Run Maelstrom tests

# 6a
./../maelstrom/maelstrom.ps1 test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

# 6b (uncomment as needed)
# ./../maelstrom/maelstrom.ps1 test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
# ./../maelstrom/maelstrom.ps1 test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
