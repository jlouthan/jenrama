package edu.princeton.sparrrow;

import org.json.JSONObject;
import java.io.IOException;
import java.net.ServerSocket;

public class ConstantExecutor extends Executor {
    public ConstantExecutor(int id, ServerSocket socketWithMonitor) throws IOException {
        super(id, socketWithMonitor);
    }
    protected TaskResultContent execute(TaskSpecContent s) {
        long start = System.currentTimeMillis();

        JSONObject spec = new JSONObject(s.getSpec());
        int timeToWait = spec.getInt("Time");
        long elapsedTime = 0;

        while(elapsedTime < timeToWait){
            elapsedTime = System.currentTimeMillis() - start;
        }

        JSONObject result = new JSONObject();
        result.put("result", elapsedTime / 1000.0);

        TaskResultContent resultC = new TaskResultContent(s.getJobID(), s.getTaskID(), s.getSchedID(), result.toString());
        return resultC;
    }
}
