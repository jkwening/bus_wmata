#!/usr/bin/env bash
cd ~/jk-apps/bus_wmata/
source wmata_env/bin/activate
python extract.py stops  # fetch stops data but with no schedules
