#!/bin/bash
set -eux

# Install dependencies on Ubuntu

apt-get update
apt-get install --yes \
  bison \
  gdb \
  libncurses5-dev
