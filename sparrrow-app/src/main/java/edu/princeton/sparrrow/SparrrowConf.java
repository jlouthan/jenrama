package edu.princeton.sparrrow;

public class SparrrowConf {

    public static final int D = 2;

    // right now, N_FRONTENDS = (# schedulers) and N_EXECUTORS = (# node monitors)
    public static final int N_FRONTENDS = 1;
    public static final int N_EXECUTORS = 1;

    public static final int PORT_0 = 32000;
    public static final int PORT_0_SCHED = 31000;
}
