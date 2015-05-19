package com.xpatterns.spark.core.java.entity;


/**
 * Created by radum on 10.04.2014.
 */

public class SparkHttpEntity {

    private String status;
    private SparkHttpEntityResult result;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public SparkHttpEntityResult getResult() {
        return result;
    }

    public void setResult(SparkHttpEntityResult result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "SparkHttpEntity [status=" + status + ", result=" + result + "]";
    }

}
