#!/bin/bash

service ssh restart || true

bash /app/start-services.sh || true

cd /app

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
pip install venv-pack

tail -f /dev/null