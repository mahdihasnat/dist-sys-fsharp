#!/bin/bash

set -eux
clear
dotnet publish -c Release
./../maelstrom/maelstrom test -w g-counter --bin ./bin/Release/net7.0/publish/GrowOnlyCounterFtCRDT --node-count 3 --rate 100 --time-limit 20 --nemesis partition
