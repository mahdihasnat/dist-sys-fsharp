#!/bin/bash

set -eux
clear
dotnet publish -c Release
# ~/Downloads/maelstrom/maelstrom test -w broadcast --bin ./bin/Release/net7.0/publish/Broadcast --node-count 1 --time-limit 20 --rate 10 --log-stderr true
~/Downloads/maelstrom/maelstrom test -w broadcast --bin ./bin/Release/net7.0/publish/Broadcast --node-count 5 --time-limit 20 --rate 10 --log-stderr true

