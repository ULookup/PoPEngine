#!/usr/bin/env bash
# 在构建目录解包官方 Ubuntu 22.04 MsQuic SDK，与项目 vcpkg 基线同版。
set -eu

version=2.4.8
case "$(dpkg --print-architecture)" in
  amd64|arm64) arch=$(dpkg --print-architecture) ;;
  *) echo "MsQuic SDK requires Ubuntu 22.04 amd64 or arm64" >&2; exit 1 ;;
esac

repo=$(cd "$(dirname "$0")/.." && pwd)
sdk="$repo/build/deps/msquic"
mkdir -p "$sdk/include" "$sdk/lib"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

base=https://packages.microsoft.com/ubuntu/22.04/prod/pool/main/libm/libmsquic
curl --fail --location --silent --show-error \
  "$base/libmsquic_${version}_${arch}.deb" -o "$scratch/package.deb"
dpkg-deb -x "$scratch/package.deb" "$scratch/package"
cp -a "$scratch"/package/usr/lib/*/libmsquic.so.2* "$sdk/lib/"
ln -sf libmsquic.so.2 "$sdk/lib/libmsquic.so"

headers="https://raw.githubusercontent.com/microsoft/msquic/v$version/src/inc"
for header in msquic.h msquic_posix.h quic_sal_stub.h; do
  curl --fail --location --silent --show-error "$headers/$header" \
    -o "$sdk/include/$header"
done
