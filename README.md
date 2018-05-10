# jenrama
Final Project for COS 518 Spring 2018:  Sparrow

## Running on the cluster

### If there are no new local changes:

1. ssh into the cluster: `ssh -i path/to/private/key/cluster_518_rsa jlouthan@ns.cs.princeton.edu`
2. Once on the cluster, see current jobs with `squeue` (there usually aren't any if you haven't started one)
3. If desired, change `NUM_SCHEDS` and `NUM_WORKERS` in start_sparrow.sh (default is 5 and 25)
4. Start sparrow on the cluster just by running the script:  `./start_sparrow.sh`

You will see the slurm logs listing each batch job ID as they start. There is a job for each scheduler and each worker. Once all the jobs are started, they can be viewed with `squeue`

#### Output

`monitor-list.txt` contains the host names of the nodes for each worker (this is used by the script and is overwritten each time)

<b>Note</b> that the following logs files take a few seconds to a minute or two to be written to, so if you check them and they are empty, let the jobs continue to run and wait a short time

`logs/` folder contains the stats logs for the schedulers you started

`sched-logs/` folder contains the stdout logs for each scheduler, labeled with their job IDs

`worker-logs/` folder contains the stdout logs for each worker, labeled with their job IDs

The logs in the three above folders are wiped and overwritten each time you run start_sparrow

### If there are new local changes to propogate to the cluster:

1. Build the project locally with `mvn package`
2. Put the latest jar on the cluster at /home/jlouthan/. This can be done however you want; one way is with scp, for example:
`scp -i /path/to/private/key/cluster_518_rsa ./sparrrow-app/target/sparrrow-app-1.0-SNAPSHOT.jar jlouthan@ns.cs.princeton.edu:/home/jlouthan/` (you will then need to enter the rsa key passphrase)

Now proceed to step 1 in the previous section

### Stopping Sparrow

Once you have run the jobs and are satisfied with the output, you can stop all the jobs by our user with `scancel --user=jlouthan`
