#!/usr/bin/env bash
cd ~/jk-apps/bus_wmata/
source wmata_env/bin/activate
# fetch routes data along with schedules and path details
python extract.py routes --sched --path
