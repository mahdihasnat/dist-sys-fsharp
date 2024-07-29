#!/bin/bash

set -eux
clear
dotnet publish -c Release
~/Downloads/maelstrom/maelstrom test -w unique-ids --bin ./bin/Release/net7.0/publish/UniqueIdGen --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr true
