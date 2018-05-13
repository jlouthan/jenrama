package edu.princeton.sparrrow;

public class SparrrowConf {

    public static final int D = 2;

    // right now, N_FRONTENDS = (# schedulers) and N_EXECUTORS = (# node monitors)
    public static final int N_FRONTENDS = 3;
    public static final int N_EXECUTORS = 4;

    public static final int PORT_0 = 32000;
    public static final int PORT_0_SCHED = 31000;
    public static final String WORKER_HOST = "127.0.0.1";
}
