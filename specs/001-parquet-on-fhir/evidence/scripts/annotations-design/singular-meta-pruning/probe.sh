#!/bin/bash
# Usage: probe.sh <Probe.java name in this folder> <args...>
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/env.sh"
P=$1; shift
"$RUN" "$HERE/$P" "$TP" "$J" "$@"
