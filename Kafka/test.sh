#!/bin/bash

set -eux
clear
echo "DOTNET_ROOT=$DOTNET_ROOT"
dotnet publish -c Release
./../maelstrom/maelstrom test -w kafka --bin ./bin/Release/net7.0/publish/Kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
# ./../maelstrom/maelstrom test -w kafka --bin ./bin/Release/net7.0/publish/Kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr
