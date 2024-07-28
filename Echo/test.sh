#!/bin/bash

set -eux
dotnet publish -c Release
~/Downloads/maelstrom/maelstrom test -w echo --bin ./bin/Release/net7.0/publish/Echo --node-count 10 --time-limit 1
~/Downloads/maelstrom/maelstrom serve