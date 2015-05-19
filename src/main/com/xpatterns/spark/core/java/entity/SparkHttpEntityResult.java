package com.xpatterns.spark.core.java.entity;


/**
 * Created by radum on 10.04.2014.
 */


public class SparkHttpEntityResult {

    private String jobId;
    private String context;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return "SparkHttpEntityResult [jobId=" + jobId + ", context=" + context
                + "]";
    }

}
