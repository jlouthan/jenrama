package edu.princeton.sparrrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * The App class launches the daemons.
 */

// import edu.princeton.sparrrow.Scheduler;

public class App {

    public static void main( String[] args ) {
        System.out.println("Starting Sparrrow App.");

        try{

            int i;
            int portCounter = 0;

            ArrayList<Scheduler> schedulers = new ArrayList<>();
            ArrayList<Frontend> fes = new ArrayList<>();
            ArrayList<NodeMonitor> monitors = new ArrayList<>();
            ArrayList<Executor> executors = new ArrayList<>();


            // Initialize streams between Scheduler and NodeMonitor
            ArrayList<Socket> schedSocketsToMonitor = new ArrayList<>();
            ArrayList<ServerSocket> monitorSocketsToSched  = new ArrayList<>();

            ServerSocket monitorSocket;
            Socket schedSocket;

            // nf * ne total pipes
            // the pipe between scheduler i and monitor j is at index i*(ne) + j - 1
            for (i = 0; i < SparrrowConf.N_FRONTENDS * SparrrowConf.N_EXECUTORS; i++){
                monitorSocket = new ServerSocket(SparrrowConf.PORT_0 + portCounter);
                schedSocket = new Socket("127.0.0.1", SparrrowConf.PORT_0 + portCounter++);

                schedSocketsToMonitor.add(schedSocket);
                monitorSocketsToSched.add(monitorSocket);
            }

            // Initialize streams between Frontend and Scheduler
            // and create Frontends and Schedulers at the same time

            ServerSocket schedSocketWithFe;
            Socket feSocketWithSched;

            ArrayList<Socket> mySocketsWithMonitor;
            int j;

            Frontend fe;
            Scheduler sched;
            for (i = 0; i < SparrrowConf.N_FRONTENDS; i++){

                schedSocketWithFe = new ServerSocket(SparrrowConf.PORT_0 + portCounter);
                feSocketWithSched = new Socket("127.0.0.1", SparrrowConf.PORT_0 + portCounter++);

                fe = new RandstatFrontend(i, feSocketWithSched);
                fes.add(fe);

                mySocketsWithMonitor = new ArrayList<>();
                for(j = 0; j < SparrrowConf.N_EXECUTORS; j++){
                    mySocketsWithMonitor.add(schedSocketsToMonitor.get(i * SparrrowConf.N_EXECUTORS + j));
                }

                sched = new Scheduler(i, schedSocketWithFe, mySocketsWithMonitor, SparrrowConf.D);
                schedulers.add(sched);
            }



            // Initialize streams between NodeMonitor and Executor
            // and create Executors and Nodemonitors at the same time

            Socket monitorSocketWithExec;
            ServerSocket execSocketWithMonitor;

            ArrayList<ServerSocket> mySocketsWithSched;

            Executor ex;
            for(i = 0; i < SparrrowConf.N_EXECUTORS; i++){
                execSocketWithMonitor = new ServerSocket(SparrrowConf.PORT_0 + portCounter);
                monitorSocketWithExec = new Socket("127.0.0.1", SparrrowConf.PORT_0 + portCounter++);

                ex = new RandstatExecutor(i, execSocketWithMonitor);
                executors.add(ex);

                mySocketsWithSched = new ArrayList<>();
                for(j = 0; j < SparrrowConf.N_FRONTENDS; j++){
                    mySocketsWithSched.add(monitorSocketsToSched.get(i + SparrrowConf.N_EXECUTORS * j));
                }

                NodeMonitor monitor = new NodeMonitor(i, mySocketsWithSched, monitorSocketWithExec);
                monitors.add(monitor);
            }


            ArrayList<Thread> allThreads = new ArrayList<>();
            Thread t;

            // Start Executors
            for(i = 0; i < executors.size(); i++){
                t = new Thread(executors.get(i), "Executor " + i);
                t.start();
                allThreads.add(t);
            }

            // Start NodeMonitors
            for(i = 0; i < monitors.size(); i++){
                t = new Thread(monitors.get(i), "Monitor " + i);
                t.start();
                allThreads.add(t);
            }

            // Start Schedulers
            for(i = 0; i < schedulers.size(); i++){
                t = new Thread(schedulers.get(i), "Scheduler " + i);
                t.start();
                allThreads.add(t);
            }

            // Start Frontends
            for(i = 0; i < fes.size(); i++){
                t = new Thread(fes.get(i), "Frontend " + i);
                t.start();
                allThreads.add(t);
            }

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
