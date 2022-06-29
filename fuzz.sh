#!/usr/bin/env bash

FUZZTIME=${FUZZTIME:-10s}

for t in `go test -list Fuzz ./pkg/parse | egrep -v '^(ok|\?)'`; do
    echo $t
    go test -fuzz="^${t}$" -fuzztime=$FUZZTIME ./pkg/parse
    echo "---"
done;
