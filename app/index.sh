#!/bin/bash
set -e

INPUT_PATH=${1:-/input/data}


/app/create_index.sh
/app/store_index.sh

echo "Full index creation and storage completed"