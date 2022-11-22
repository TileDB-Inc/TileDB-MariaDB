#!/bin/bash
set -eux

# Install dependencies on macOS

brew install \
  bison@2.7 \
  boost \
  cmake \
  gnutls \
  jemalloc \
  m4 \
  openssl@1.1 \
  traildb/judy/judy
