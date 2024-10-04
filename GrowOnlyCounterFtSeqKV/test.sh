#!/bin/bash

set -eux
clear
export DOTNET_ROOT=/usr/lib/dotnet
dotnet publish -c Release
./../maelstrom/maelstrom test -w g-counter --bin ./bin/Release/net7.0/publish/GrowOnlyCounterFtSeqKV --node-count 3 --rate 100 --time-limit 20 --nemesis partition
