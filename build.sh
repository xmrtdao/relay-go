#!/bin/bash
# Build script for Go Relay Agent Daemon
# Usage: ./build.sh [target]
#   target: windows (default), linux, arm64, all

set -e

VERSION="0.1.0"
BUILD_DIR="build"
BINARY="relayd"

mkdir -p "$BUILD_DIR"

build() {
    local GOOS="$1"
    local GOARCH="$2"
    local suffix="$3"
    local target="$4"

    echo "Building ${BINARY}${suffix} (${GOOS}/${GOARCH})..."
    GOOS=$GOOS GOARCH=$GOARCH go build \
        -ldflags="-X main.version=${VERSION} -s -w" \
        -o "${BUILD_DIR}/${BINARY}${suffix}" \
        ./cmd/relayd/
    echo "  → ${BUILD_DIR}/${BINARY}${suffix}"
}

case "${1:-windows}" in
    windows)
        build windows amd64 ".exe" "Windows x86_64"
        ;;
    linux)
        build linux amd64 "" "Linux x86_64"
        ;;
    arm64)
        build linux arm64 "-arm64" "Linux ARM64"
        ;;
    all)
        build windows amd64 ".exe" "Windows x86_64"
        build linux amd64 "" "Linux x86_64"
        build linux arm64 "-arm64" "Linux ARM64 (Hermes)"
        ;;
    *)
        echo "Usage: $0 [windows|linux|arm64|all]"
        exit 1
        ;;
esac

echo ""
echo "Build complete. Binaries in ${BUILD_DIR}/"
ls -lh "${BUILD_DIR}/"
