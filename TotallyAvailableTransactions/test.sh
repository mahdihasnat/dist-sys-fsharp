#!/usr/bin/bash

set -eux
clear

dotnet publish -c Release
# 6a
./../maelstrom/maelstrom test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

# 6b
# ./../maelstrom/maelstrom test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
# ./../maelstrom/maelstrom test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition

