#!/usr/bin/env bash
cd ~/jk-apps/wmata/
source venv/bin/activate
python fetch_data.py schedule -r all
