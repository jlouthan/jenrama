#!/bin/bash
#SBATCH -n 1 # Number of cores
#SBATCH -N 1 # Ensure that all cores are on one machine
#SBATCH -t 0-00:05 # Runtime in D-HH:MM
#SBATCH --mem=100 # Memory pool for all cores (see also --mem-per-cpu)
#SBATCH -o ./sched-logs/sched_%j.out # File to which STDOUT will be written
#SBATCH -e ./sched-logs/sched_%j.err # File to which STDERR will be written

#exec java -cp ./sparrrow-app-1.0-SNAPSHOT.jar edu.princeton.sparrrow.CreateScheduler 0 1 1 ns-130

exec java -cp ./sparrrow-app-1.0-SNAPSHOT.jar edu.princeton.sparrrow.CreateScheduler "$@"
