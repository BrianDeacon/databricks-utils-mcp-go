#!/usr/bin/env bash
set -euo pipefail

REPO="BrianDeacon/databricks-utils-mcp-go"
BINARY="databricks-utils-mcp"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64)  ARCH="amd64" ;;
  aarch64) ARCH="arm64" ;;
  arm64)   ARCH="arm64" ;;
  *)
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac

case "$OS" in
  linux|darwin) ;;
  *)
    echo "Unsupported OS: $OS (use Windows releases manually)" >&2
    exit 1
    ;;
esac

VERSION="${VERSION:-$(curl -sfL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)}"
if [ -z "$VERSION" ]; then
  echo "Failed to determine latest version" >&2
  exit 1
fi

PROJECT="databricks-utils-mcp-go"
TARBALL="${PROJECT}_${VERSION#v}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${TARBALL}"

echo "Installing ${BINARY} ${VERSION} (${OS}/${ARCH})..."

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

curl -sfL "$URL" -o "$TMPDIR/$TARBALL"
tar -xzf "$TMPDIR/$TARBALL" -C "$TMPDIR"

if [ -w "$INSTALL_DIR" ]; then
  mv "$TMPDIR/$BINARY" "$INSTALL_DIR/$BINARY"
else
  echo "Requires sudo to install to $INSTALL_DIR"
  sudo mv "$TMPDIR/$BINARY" "$INSTALL_DIR/$BINARY"
fi

chmod +x "$INSTALL_DIR/$BINARY"
echo "Installed $INSTALL_DIR/$BINARY"
