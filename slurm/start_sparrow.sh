#!/bin/bash

# This script uses sbatch to create the number of schedulers and workers, specified below, 
# each as separate processes on separate nodes in the cluster

NUM_SCHEDS=5
NUM_WORKERS=25

SCHEDS_CREATED=0
WORKERS_CREATED=0

WORKER_HOSTS_FILE="./monitor-list.txt"

# Ensure we start with a fresh list of worker hosts
rm $WORKER_HOSTS_FILE

# Remove previous worker and sched stdout logs
rm ./worker-logs/*
rm ./sched-logs/*

# Create workers one by one
while [ $WORKERS_CREATED -lt $NUM_WORKERS ]; do
	sbatch launch_worker.sh $WORKERS_CREATED $NUM_WORKERS $NUM_SCHEDS
	let WORKERS_CREATED=WORKERS_CREATED+1
	sleep 1 # pause to be kind to the scheduler
done

sleep 30 # pause to give monitors a chance to start and open all sockets

# Create schedulers one by one
while [ $SCHEDS_CREATED -lt $NUM_SCHEDS ]; do
	sbatch launch_scheduler.sh $SCHEDS_CREATED $NUM_WORKERS $NUM_SCHEDS $WORKER_HOSTS_FILE
	let SCHEDS_CREATED=SCHEDS_CREATED+1
done
