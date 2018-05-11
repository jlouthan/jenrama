#!/bin/bash

# This script is a WIP that will allow us to specify the specific nodes each worker is on
# We may not even need it, but putting in repo anyway

# This script uses sbatch to create the number of schedulers and workers, specified below, 
# each as separate processes on separate nodes in the cluster

NUM_SCHEDS=5
NUM_WORKERS=25

SCHEDS_CREATED=0
WORKERS_CREATED=0

WORKER_HOSTS_FILE="./monitor-list.txt"

#available hosts
declare -a hostarray=("082" "083" "084" "085" "086" "087" "088" "089" "090" "091" "092" "093" "094" "095" "096" "097" "098" "099" "100" "101" "102" "103" "104" "105" "106" "107" "108" "109" "110" "111" "112" "113" "114" "115" "116" "117" "118" "119" "120" "130" "133" "134" "135" "136" "137" "138" "140" "141" "142" "144" "146" "147" "148" "150" "151" "153")

# Ensure we start with a fresh list of worker hosts
rm $WORKER_HOSTS_FILE

# Remove previous worker and sched stdout logs
rm ./worker-logs/*
rm ./sched-logs/*

# Create workers one by one
while [ $WORKERS_CREATED -lt $NUM_WORKERS ]; do
	sbatch --nodelist=ns-${hostarray[$WORKERS_CREATED]} launch_worker.sh $WORKERS_CREATED $NUM_WORKERS $NUM_SCHEDS
	let WORKERS_CREATED=WORKERS_CREATED+1
	sleep 5 # pause to be kind to the scheduler
done

sleep 5 # pause to give monitors a chance to start and open all sockets

# Create schedulers one by one
while [ $SCHEDS_CREATED -lt $NUM_SCHEDS ]; do
	sbatch launch_scheduler.sh $SCHEDS_CREATED $NUM_WORKERS $NUM_SCHEDS $WORKER_HOSTS_FILE
	let SCHEDS_CREATED=SCHEDS_CREATED+1
done
