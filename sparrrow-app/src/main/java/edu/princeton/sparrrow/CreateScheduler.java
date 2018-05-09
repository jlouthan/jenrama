package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Creates and Launches a single scheduler daemon: 1 scheduler and 1 instance of our frontend
 */


public class CreateScheduler {

    public static void main( String[] args ) {
        System.out.println("Starting a Scheduler");

        // TODO replace with command line input or default to config value
        final int port0 = SparrrowConf.PORT_0;
        // TODO do we need this? if so, name better
        final int port0_sched = SparrrowConf.PORT_0_SCHED;

        // TODO replace with command line input or default to config value
        final int num_monitors = SparrrowConf.N_EXECUTORS;
        final int num_scheds = SparrrowConf.N_FRONTENDS;

        System.out.println("Listening to " + num_monitors + " monitors");

        // TODO replace with command line input
        final int sched_id = 0;

        try{

            int portCounter = 0;

            ArrayList<Scheduler> schedulers = new ArrayList<>();
            ArrayList<Frontend> fes = new ArrayList<>();

            // Initialize connections to monitor sockets from this scheduler
            ArrayList<Socket> schedSocketsWithMonitor = new ArrayList<>();
            Socket schedSocket;

            // Create client connection to the designated socket on each monitor
            for (int i = 0; i < num_monitors; i++){
                // each worker uses (num_scheds + 1) ports: 1 per sched and 1 for the executor
                int monitorPortOffset = i * (num_scheds + 1) + sched_id;
                System.out.println("Trying to create socket with port " + (port0 + monitorPortOffset));
                schedSocket = new Socket("127.0.0.1", port0 + monitorPortOffset);
                System.out.println("created socket with port " + (port0 + monitorPortOffset));
                schedSocketsWithMonitor.add(schedSocket);
            }

            // There is one frontend with the scheduler & the scheduler has a socket to talk to it
            ServerSocket schedSocketWithFe = new ServerSocket(port0_sched + sched_id);
            Socket feSocketWithSched = new Socket("127.0.0.1", port0_sched + sched_id);

            Frontend frontend = new RandstatFrontend(sched_id, feSocketWithSched);
            Scheduler scheduler = new Scheduler(sched_id, schedSocketWithFe, schedSocketsWithMonitor, SparrrowConf.D);

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
