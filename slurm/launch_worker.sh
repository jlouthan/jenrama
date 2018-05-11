#!/bin/bash
#SBATCH -n 1 # Number of cores
#SBATCH -N 1 # Ensure that all cores are on one machine
#SBATCH -t 0-00:05 # Runtime in D-HH:MM
#SBATCH --mem=100 # Memory pool for all cores (see also --mem-per-cpu)
#SBATCH -o worker-logs/worker_%j.out # File to which STDOUT will be written
#SBATCH -e worker-logs/worker_%j.err # File to which STDERR will be written

# Save the host name of the node this process gets assigned to, to be referenced by schedulers
echo "$SLURM_JOB_NODELIST" >> monitor-list.txt

exec java -cp ./sparrrow-app-1.0-SNAPSHOT.jar edu.princeton.sparrrow.CreateNodeMonitor "$@"
