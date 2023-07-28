#!/bin/bash

cd .. && cd each && python -m venv .venv && source activate.sh && pip install -r requirements.txt && cp ../gcs_key/key.json ./key.json


