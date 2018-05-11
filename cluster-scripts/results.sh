#!/bin/bash

# Syntax: <all log files to test>

total_jobs=0
total_tasks=0
total_time=0

#TODO: insert avg response time, and total number of jobs to file, and check there first

# walk through each file
for var in "$@"
do
    scheduler_jobs=0
    scheduler_tasks=0
    scheduler_time=0
    # walk through lines
    while read -r line || [[ -n "$line" ]]; do
	# if line has '[response time]' then get r_time
	regex1='\[response time\]'
	if [[ $line =~ $regex1 ]]; then
	    # add r_time to total_time
	    r_time=`echo "$line" | grep -o  '[0-9]\+' | tr -d '[:space:]'  | tr -d '0'`
	    let total_time=$total_time+$r_time
	    let scheduler_time=$scheduler_time+$r_time
	    
	    # increment total_jobs
	    let total_jobs=$total_jobs+1
	    let scheduler_jobs=$scheduler_jobs+1
	    
	# if line has '[number of tasks]' then get num_tasks
	regex2='\[number of tasks\]'
	elif [[ $line =~ $regex2 ]]; then
	    # add num_tasks to total_tasks
	    num_tasks=`echo "$line" | grep -o  '[0-9]\+'`
	    let total_tasks=$total_tasks+$num_tasks
	    let scheduler_tasks=$scheduler_tasks+$num_tasks
	fi
    done < $var
    echo $var "stats"
    echo " total number of jobs =" $scheduler_jobs
    echo " total number of tasks =" $scheduler_tasks
    case "$total_tasks" in
	0)
	    scheduler_avg_r_time=0
	    ;;
	*)
	    let scheduler_avg_r_time=$scheduler_time/$scheduler_jobs
	    ;;
    esac
    echo " average response time per job =" $scheduler_avg_r_time "ms"
    echo ""
done

# print out total_jobs, total_tasks, and total_time/total_tasks
echo "FINAL STATS"
echo "total number of jobs =" $total_jobs
echo "total number of tasks =" $total_tasks

case "$total_tasks" in
    0)
	avg_r_time=0
	;;
    *)
	let avg_r_time=$total_time/$total_jobs
	;;
esac

echo "average response time per job =" $avg_r_time "ms"

# TODO: also print this to a file for the greatest common prefix of the arguments
