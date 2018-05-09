#!/bin/bash

# This script uses sbatch to create the number of schedulers and workers, specified below, 
# each as separate processes on separate nodes in the cluster

NUM_SCHEDS=1
NUM_WORKERS=1

SCHEDS_CREATED=0
WORKERS_CREATED=0

WORKER_HOSTS_FILE="./monitor-list.txt"

while [ $WORKERS_CREATED -lt $NUM_WORKERS ]; do
	sbatch launch_worker.sh $WORKERS_CREATED $NUM_WORKERS $NUM_SCHEDS
	let WORKERS_CREATED=WORKERS_CREATED+1
	sleep 1 # pause to be kind to the scheduler
done

sleep 5 # pause to give monitors a chance to start and open all sockets

while [ $SCHEDS_CREATED -lt $NUM_SCHEDS ]; do
	sbatch launch_scheduler.sh $SCHEDS_CREATED $NUM_WORKERS $NUM_SCHEDS ns-130
	let SCHEDS_CREATED=SCHEDS_CREATED+1
done
