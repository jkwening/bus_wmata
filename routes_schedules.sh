#!/usr/bin/env bash
cd ~/jk-apps/wmata/
source wmata_env/bin/activate
python fetch_data.py schedule -r all
