#!/bin/bash

# Syntax: <all log files to test>

total_jobs=0
total_tasks=0
total_time=0

# get greatest common prefix for arguments
prefix=$1
for arg in "$@"; do
    prefix=`printf "%s\n%s\n" "$prefix" "$arg" | sed -e 'N;s/^\(.*\).*\n\1.*$/\1/'`
done

# print output to a file for the greatest common prefix
filebasename=`basename $prefix | sed 's/_$//'` # cut off final underscore
dirsname=`dirname $prefix`
filename=$dirsname"/results_"$filebasename"s.txt"

# walk through each file
for var in "$@"; do
    scheduler_jobs=0
    scheduler_tasks=0
    scheduler_time=0

    # walk through lines
    while read -r line || [[ -n "$line" ]]; do
        regex_rt='\[response time\]'
        regex_nt='\[number of tasks\]'

        # if line has '[response time]' then get r_time
        if [[ $line =~ $regex_rt ]]; then
            # add r_time to total_time
            r_time=`echo "$line" | grep -o  '[0-9]\+' | tr -d '[:space:]'  | tr -d '0'`
            let total_time=$total_time+$r_time
            let scheduler_time=$scheduler_time+$r_time
            
            # increment total_jobs
            let total_jobs=$total_jobs+1
            let scheduler_jobs=$scheduler_jobs+1
            
        # if line has '[number of tasks]' then get num_tasks
        elif [[ $line =~ $regex_nt ]]; then
            # add num_tasks to total_tasks
            num_tasks=`echo "$line" | grep -o  '[0-9]\+'`
            let total_tasks=$total_tasks+$num_tasks
            let scheduler_tasks=$scheduler_tasks+$num_tasks
        fi
    done < $var
    
    # print stats for file
    echo $var "stats" >> $filename
    echo " total number of jobs =" $scheduler_jobs >> $filename
    echo " total number of tasks =" $scheduler_tasks >> $filename
    case "$total_tasks" in
        0)
            scheduler_avg_r_time=0
            ;;
        *)
            let scheduler_avg_r_time=$scheduler_time/$scheduler_jobs
            ;;
    esac
    echo " average response time per job =" $scheduler_avg_r_time "ms" >> $filename
    echo "" >> $filename
done

# print out total_jobs, total_tasks, and total_time/total_tasks
echo "FINAL STATS" >> $filename
echo "total number of jobs =" $total_jobs >> $filename
echo "total number of tasks =" $total_tasks >> $filename

case "$total_tasks" in
    0)
        avg_r_time=0
        ;;
    *)
        let avg_r_time=$total_time/$total_jobs
        ;;
esac

echo "average response time per job =" $avg_r_time "ms" >> $filename

# also print results to terminal
cat $filename
