package com.xpatterns.spark.core.java.instrumentation.metrics.jmx;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cristianf on 3/24/15.
 */
public class Snapshot {
    public long timestamp;
    public AtomicLong items = new AtomicLong();
    public AtomicLong milliseconds = new AtomicLong();

    public Snapshot(long timestamp) {
        this.timestamp = timestamp;
    }
}
