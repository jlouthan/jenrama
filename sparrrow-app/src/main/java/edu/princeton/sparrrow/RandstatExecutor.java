package edu.princeton.sparrrow;

import org.json.JSONObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.IntSummaryStatistics;
import java.util.Random;


public class RandstatExecutor extends Executor {
    public RandstatExecutor(int id, ServerSocket socketWithMonitor) throws IOException {
        super(id, socketWithMonitor);
    }

    protected TaskResultContent execute(TaskSpecContent s){
        JSONObject mySpec = new JSONObject(s.getSpec());
        int maxVal = mySpec.getInt("maxVal");
        int num = mySpec.getInt("num");
        long seed = mySpec.getLong("seed");

        IntSummaryStatistics summary = new IntSummaryStatistics();
        Random r = new Random(seed);
        int i = 0;

        for(i = 0; i < num; i++){
            summary.accept(r.nextInt(maxVal));
        }

        JSONObject result = new JSONObject();
        result.put("sum", summary.getSum());
        result.put("count", summary.getCount());
        result.put("mean", summary.getAverage());

        TaskResultContent myResult = new TaskResultContent(s.getJobID(), s.getTaskID(), s.getSchedID(), result.toString());
        return myResult;
    }
}
