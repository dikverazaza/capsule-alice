#!/bin/sh
# Sourced by /etc/init.d/rcS at boot (rcS sources *.sh files).
# Start the Alice stack after the rest of the system is up.

/usr/bin/capsula-alice-launcher.sh >/tmp/capsula-alice-launcher.log 2>&1 &
