package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Creates and Launches a single scheduler daemon: 1 scheduler and 1 instance of our frontend
 *
 * usage:  java CreateScheduler schedId numMonitors numSchedulers
 * all args will are optional and will be set to defaults in SparrrowConf otherwise
 *
 * Note:  Workers should be created before schedulers so that the server sockets are ready for client connections
 */


public class CreateScheduler {

    public static void main( String[] args ) {

        final int port0 = SparrrowConf.PORT_0;
        final int port0_sched = SparrrowConf.PORT_0_SCHED;

        int schedId = 0;
        int numMonitors = SparrrowConf.N_EXECUTORS;
        int numScheds = SparrrowConf.N_FRONTENDS;
        String workerHost = SparrrowConf.WORKER_HOST;

        // read in provided command line arguments
        if (args.length > 0) {
            schedId = Integer.parseInt(args[0]);
            if (args.length > 1) {
                numMonitors = Integer.parseInt(args[1]);
                if (args.length > 2) {
                    numScheds = Integer.parseInt(args[2]);
                    if (args.length > 3) {
                        workerHost = args[3];
                    }
                }
            }
        }

        System.out.println("Starting Scheduler " + schedId + " listening to " + numMonitors + " monitors");

        try{

            int portCounter = 0;

            ArrayList<Scheduler> schedulers = new ArrayList<>();
            ArrayList<Frontend> fes = new ArrayList<>();

            // Initialize connections to monitor sockets from this scheduler
            ArrayList<Socket> schedSocketsWithMonitor = new ArrayList<>();
            Socket schedSocket;

            // Create client connection to the designated socket on each monitor
            for (int i = 0; i < numMonitors; i++){
                // each worker uses (num_scheds + 1) ports: 1 per sched and 1 for the executor
                int monitorPortOffset = i * (numScheds + 1) + schedId;
                System.out.println("Trying to create socket with port " + (port0 + monitorPortOffset));
                schedSocket = new Socket(workerHost, port0 + monitorPortOffset);
                System.out.println("created socket with port " + (port0 + monitorPortOffset));
                schedSocketsWithMonitor.add(schedSocket);
            }

            // There is one frontend with the scheduler & the scheduler has a socket to talk to it
            ServerSocket schedSocketWithFe = new ServerSocket(port0_sched + schedId);
            Socket feSocketWithSched = new Socket("127.0.0.1", port0_sched + schedId);

            Frontend frontend = new RandstatFrontend(schedId, feSocketWithSched);
            Scheduler scheduler = new Scheduler(schedId, schedSocketWithFe, schedSocketsWithMonitor, SparrrowConf.D);

            ArrayList<Thread> allThreads = new ArrayList<>();

            // Create and start the threads
            Thread schedulerThread = new Thread(scheduler, "Scheduler " + scheduler.id);
            schedulerThread.start();
            allThreads.add(schedulerThread);

            Thread frontendThread = new Thread(frontend, "Frontend " + frontend.id);
            frontendThread.start();
            allThreads.add(frontendThread);

            // Await all threads finishing (unused for now)
            for(Thread th: allThreads){
                //th.join();
            }

        } catch (IOException e){
            e.printStackTrace();
            System.out.println("App broke");
        }

    }
}
