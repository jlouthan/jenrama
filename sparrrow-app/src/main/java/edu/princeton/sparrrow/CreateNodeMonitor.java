package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Creates and Launches a single worker daemon: 1 node monitor and 1 instance of our executor
 *
 * usage:  java CreateWorker workerId numMonitors numSchedulers
 *  * all args will are optional and will be set to defaults in SparrrowConf otherwise
 *
 *  Note:  Workers should be created before schedulers so that the server sockets are ready for client connections
 */

public class CreateNodeMonitor {

    public static void main( String[] args ) {

        final int port0 = SparrrowConf.PORT_0;

        int workerId = 0;
        int numMonitors = SparrrowConf.N_EXECUTORS;
        int numScheds = SparrrowConf.N_FRONTENDS;

        // read in provided command line arguments
        if (args.length > 0) {
            workerId = Integer.parseInt(args[0]);
            if (args.length > 1) {
                numMonitors = Integer.parseInt(args[1]);
                if (args.length > 2) {
                    numScheds = Integer.parseInt(args[2]);
                }
            }
        }

        System.out.println("Starting Worker " + workerId + " listening to " + numScheds + " schedulers");

        try{

            // The ith worker has ports starting at (port0 + (worker_id * (num_scheds + 1)))
            int portCounter = workerId * (numScheds + 1);

            // Initialize sockets between this monitor and all schedulers
            ArrayList<ServerSocket> monitorSocketsWithSched  = new ArrayList<>();
            ServerSocket monitorSocket;

            // The monitor has a socket for each scheduler to use
            for (int i = 0; i < numScheds; i++){
                monitorSocket = new ServerSocket(port0 + portCounter);
                System.out.println("Worker " + workerId + " created socket on port " + (port0 + portCounter) + " for scheduler " + i);
                monitorSocketsWithSched.add(monitorSocket);
                portCounter++;
            }

            // There is one executor in the worker, it has a socket to talk w/ the monitor
            ServerSocket execSocketWithMonitor = new ServerSocket(port0 + portCounter);
            Socket monitorSocketWithExec = new Socket("127.0.0.1", port0 + portCounter);

            Executor executor = new RandstatExecutor(workerId, execSocketWithMonitor);
            NodeMonitor monitor = new NodeMonitor(workerId, monitorSocketsWithSched, monitorSocketWithExec);

            ArrayList<Thread> allThreads = new ArrayList<>();

            // Create and start the threads
            Thread executorThread = new Thread(executor, "Executor " + executor.id);
            executorThread.start();
            allThreads.add(executorThread);

            Thread monitorThread = new Thread(monitor, "Monitor " + monitor.id);
            monitorThread.start();
            allThreads.add(monitorThread);

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
