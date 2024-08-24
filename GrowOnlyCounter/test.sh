#!/bin/bash

set -eux
clear
dotnet publish -c Release
~/Downloads/maelstrom/maelstrom test -w g-counter --bin ./bin/Release/net7.0/publish/GrowOnlyCounter --node-count 3 --rate 100 --time-limit 20 --nemesis partition --log-stderr

