package com.xpatterns.spark.core.java.job;


/**
 * Created by radum on 10.04.2014.
 */

public class SparkJobStatus {

    private String jobId;
    private String duration;
    private String classPath;
    private String startTime;
    private String context;
    private String status;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }


    @Override
    public String toString() {
        return "SparkJobStatus [jobId=" + jobId + ", duration=" + duration
                + ", classPath=" + classPath + ", startTime=" + startTime
                + ", context=" + context + ", status=" + status;
    }

}
