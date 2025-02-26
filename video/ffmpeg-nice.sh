#!/bin/bash -ex
# Run ffmpeg with low CPU (nice) and IO (ionice) priority
# so it doesn't interfere with other processes too much
nice -n19 ionice -c2 -n7 ffmpeg "$@"
