package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Creates and Launches a single worker daemon: 1 node monitor and 1 instance of our executor
 */

public class CreateNodeMonitor {

    public static void main( String[] args ) {
        System.out.println("Starting Node Monitor");

        // TODO replace with command line input or default to config value
        final int port0 = SparrrowConf.PORT_0;

        // TODO replace with command line input or default to config value??
        final int num_scheds = SparrrowConf.N_FRONTENDS;
        final int num_monitors = SparrrowConf.N_EXECUTORS;

        System.out.println("Listening to " + num_scheds + " schedulers");

        // TODO replace with command line input
        final int worker_id = 0;

        try{

            // The ith worker has ports starting at (port0 + (worker_id * (num_scheds + 1)))
            int portCounter = worker_id * (num_scheds + 1);

            // Initialize sockets between this monitor and all schedulers
            ArrayList<ServerSocket> monitorSocketsWithSched  = new ArrayList<>();
            ServerSocket monitorSocket;

            // The monitor has a socket for each scheduler to use
            for (int i = 0; i < num_scheds; i++){
                monitorSocket = new ServerSocket(port0 + portCounter);
                monitorSocketsWithSched.add(monitorSocket);
                portCounter++;
            }

            // There is one executor in the worker, it has a socket to talk w/ the monitor
            ServerSocket execSocketWithMonitor = new ServerSocket(port0 + portCounter);
            Socket monitorSocketWithExec = new Socket("127.0.0.1", port0 + portCounter);

            Executor executor = new RandstatExecutor(worker_id, execSocketWithMonitor);
            NodeMonitor monitor = new NodeMonitor(worker_id, monitorSocketsWithSched, monitorSocketWithExec);

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
