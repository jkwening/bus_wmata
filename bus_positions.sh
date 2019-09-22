#!/usr/bin/env bash
cd ~/jk-apps/wmata/
source venv/bin/activate
python fetch_data.py position -i 5280  # update every ~15 secs for 22 hours
