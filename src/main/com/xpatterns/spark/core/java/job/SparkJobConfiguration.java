package com.xpatterns.spark.core.java.job;


/**
 * Created by radum on 10.04.2014.
 */
public class SparkJobConfiguration {

    private Object configuration;

    public Object getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Object configuration) {
        this.configuration = configuration;
    }

    @Override
    public String toString() {
        return "SparkJobConfiguration [configuration=" + configuration + "]";
    }


}
