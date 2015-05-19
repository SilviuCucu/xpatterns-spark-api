package com.xpatterns.spark.core.java.instrumentation.metrics.data;

/**
 * Created by radum on 16/01/15.
 */
public interface MetricsProgressItem {

    public void incrementProcessedItemsBy(long deltaValue, long elapsedTime);

    public void incrementFailedItemsBy(long deltaValue);

    public void reset();
}
