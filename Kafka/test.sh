#!/bin/bash

set -eux
clear
dotnet publish -c Release
# ~/Downloads/maelstrom/maelstrom test -w kafka --bin ./bin/Release/net7.0/publish/Kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr
~/Downloads/maelstrom/maelstrom test -w kafka --bin ./bin/Release/net7.0/publish/Kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr
