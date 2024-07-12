#!/bin/bash

SHARED_DIR="shared_dir"

podman run --rm -v "$(pwd)":/${SHARED_DIR} cloud-ip-prefixes --collect --upload -log-file="${SHARED_DIR}/output.log"
