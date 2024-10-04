#!/usr/bin/bash

set -eux
clear
export DOTNET_ROOT=/usr/lib/dotnet
dotnet publish -c Release
./../maelstrom/maelstrom test -w txn-rw-register --bin ./bin/Release/net7.0/publish/TotallyAvailableTransactions --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
