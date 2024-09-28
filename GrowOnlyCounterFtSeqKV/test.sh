#!/bin/bash

set -eux
clear
echo "DOTNET_ROOT=$DOTNET_ROOT"
dotnet publish -c Release
for i in {1..1};
do
    echo $i
    ./../maelstrom/maelstrom test -w g-counter --bin ./bin/Release/net7.0/publish/GrowOnlyCounterFtSeqKV --node-count 3 --rate 100 --time-limit 20 --nemesis partition
done