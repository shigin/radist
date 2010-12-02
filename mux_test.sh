#!/usr/bin/env bash

for i in `seq 20 2>/dev/null || jot 20` ; do
    echo $0 $1 $i
#    sleep 1
done
