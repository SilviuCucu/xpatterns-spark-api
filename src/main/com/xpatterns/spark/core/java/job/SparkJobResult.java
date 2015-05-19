package com.xpatterns.spark.core.java.job;


/**
 * Created by radum on 10.04.2014.
 */

public class SparkJobResult {

    private String status;
    private Object result;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "SparkJobSyncResult [status=" + status + ", result=" + result
                + "]";
    }

}
