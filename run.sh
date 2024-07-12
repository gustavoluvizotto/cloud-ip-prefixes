#!/bin/bash

SHARED_DIR="shared_dir"
IP_PREFIXES="ip_prefixes"

mkdir -p "${IP_PREFIXES}"
podman run --net=host --rm -v "$(pwd)"/${IP_PREFIXES}:/${IP_PREFIXES} -v "$(pwd)":/${SHARED_DIR} cloud-ip-prefixes --collect --upload -log-file="${SHARED_DIR}/output.log"

