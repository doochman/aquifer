#!/bin/sh
# Compatibility shim: `./up.sh --doctor` and friends now run the cross-platform launcher.
[ $# -gt 0 ] || set -- up
exec "$(dirname "$0")/lakehouse" "$@"
