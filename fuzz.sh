#!/usr/bin/env bash

for t in `go test -list Fuzz ./pkg/parse | egrep -v '^(ok|\?)'`; do
    echo $t
    go test -fuzz="^${t}$" -fuzztime=10s ./pkg/parse
    echo "---"
done;
