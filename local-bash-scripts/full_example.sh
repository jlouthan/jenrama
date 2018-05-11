#!/bin/bash

NUM_SCHEDS=2
NUM_WORKERS=3

# This script creates the above number of schedulers and workers, each as separate processes
#
# Note that killing this script will not kill all the child processes!! You must
# do that yourself right now.

SCHEDS_CREATED=0
WORKERS_CREATED=0

while [ $WORKERS_CREATED -lt $NUM_WORKERS ]; do
	sh launch_worker.sh $WORKERS_CREATED $NUM_WORKERS $NUM_SCHEDS &
	let WORKERS_CREATED=WORKERS_CREATED+1
done

sleep 5

while [ $SCHEDS_CREATED -lt $NUM_SCHEDS ]; do
	sh launch_scheduler.sh $SCHEDS_CREATED $NUM_WORKERS $NUM_SCHEDS &
	let SCHEDS_CREATED=SCHEDS_CREATED+1
done