#!/usr/bin/env bash
cd ~/jk-apps/wmata/
source venv/bin/activate
python fetch_data.py position -i 4000  # update every ~20 secs for 22 hours (3960 + overage)
