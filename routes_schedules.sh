#!/usr/bin/env bash
cd ~/jk-apps/bus_wmata/
source wmata_env/bin/activate
python extract.py routes --sched  # fetch routes data along with schedules
