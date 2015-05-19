package com.xpatterns.spark.core.java.instrumentation.metrics.data;

/**
 * Created by radum on 16/01/15.
 */
public interface MetricsStatisticsItem {

    public double getThroughput();

    public double getAverageLatency();

    public long getTotalProcessedItems();

    public long getTotalFailedItems();

    public long getTotalCalls();
}
