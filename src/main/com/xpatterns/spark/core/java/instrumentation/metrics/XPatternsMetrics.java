package com.xpatterns.spark.core.java.instrumentation.metrics;

import com.xpatterns.spark.core.java.instrumentation.metrics.jmx.Snapshot;
import com.xpatterns.spark.core.java.instrumentation.metrics.jmx.XPatternsMetricsItemMXBean;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by radum on 12/16/14
 */
public class XPatternsMetrics implements XPatternsMetricsItemMXBean, Serializable {

    private static Logger logger = Logger.getLogger(XPatternsMetrics.class);

    private final static int SNAPSHOT_COUNT = 14;
    // the sum of the weights must always be 1
    private final static float[] SNAPSHOT_WEIGHTS = new float[]{0.4f, 0.2f, 0.1f, 0.05f, 0.04f, 0.03f, 0.03f, 0.03f, 0.02f, 0.02f, 0.02f, 0.02f, 0.02f, 0.01f, 0.01f};

    private AtomicLong totalFailed = new AtomicLong();
    private AtomicLong totalItems = new AtomicLong();
    private AtomicLong totalCalls = new AtomicLong();

    // latest snapshot, the one we currently save metrics to
    private Snapshot snapshot = new Snapshot(System.currentTimeMillis() / 1000 * 1000);
    // sliding window metrics history
    private Snapshot snapshots[] = new Snapshot[SNAPSHOT_COUNT];

    public String getVersion() {
        return "v1.0";
    }

    public long getTotalCalls() {
        return totalCalls.get();
    }

    public long getTotalProcessedItems() {
        return totalItems.get();
    }

    @Override
    public long getTotalFailedItems() {
        return totalFailed.get();
    }

    public double getAverageLatency() {
        updateSnapshots();
        double result = 0;
        double buffer;
        for (int i = 0; i < SNAPSHOT_COUNT; i++) {// compute weighted average
            if (snapshots[i] != null) {
                buffer = (snapshots[i].milliseconds.doubleValue() / snapshots[i].items.doubleValue()) * SNAPSHOT_WEIGHTS[i + 1];
                if (!Double.isNaN(buffer) && (!Double.isInfinite(buffer))) {
                    result += buffer;
                }
            }
        }
        // add current value to the weighted average
        buffer = (snapshot.milliseconds.doubleValue() / snapshot.items.doubleValue()) * SNAPSHOT_WEIGHTS[0];
        if (!Double.isNaN(buffer) && (!Double.isInfinite(buffer))) {
            result += buffer;
        }
        return result;
    }

    public double getThroughput() {
        updateSnapshots();
        double result = 0;
        for (int i = 0; i < SNAPSHOT_COUNT; i++) {// compute weighted average
            if (snapshots[i] != null) {
                result += snapshots[i].items.doubleValue() * SNAPSHOT_WEIGHTS[i + 1];
            }
        }
        // add most recent value to the weighted average
        result += snapshot.items.doubleValue() * SNAPSHOT_WEIGHTS[0];
        return result;
    }

    @Override
    public void incrementProcessedItemsBy(long deltaValue, long elapsedTime) {
        totalCalls.addAndGet(1);
        totalItems.addAndGet(deltaValue);
        updateSnapshots();
        snapshot.items.addAndGet(deltaValue);
        snapshot.milliseconds.addAndGet(elapsedTime);
    }

    @Override
    public void incrementFailedItemsBy(long deltaValue) {
        totalFailed.addAndGet(deltaValue);
    }

    public void reset() {
        totalItems.set(0);
        totalCalls.set(0);
        totalFailed.set(0);
        snapshot = new Snapshot(System.currentTimeMillis() / 1000 * 1000);
        snapshots = new Snapshot[SNAPSHOT_COUNT];
    }


    private void updateSnapshots() {
        long currentTime = System.currentTimeMillis();
        if (snapshot.timestamp + 1000 < currentTime) { //if the last interval start is over one second old
            updateSnapshotsInternal();
        }
    }

    private synchronized void updateSnapshotsInternal() {
        long currentTime = System.currentTimeMillis();
        if (snapshot.timestamp + 1000 < currentTime) { //if the last interval start is over one second old
            // shift values with number of seconds passed since last interval start, this is usually 1 unless the method was not called for more than a second
            long shift = currentTime / 1000 - snapshot.timestamp / 1000;
            for (int i = 0; i < Math.min(shift, SNAPSHOT_COUNT); i++) { // shift values
                for (int j = SNAPSHOT_COUNT - 2; j >= 0; j--) {
                    snapshots[j + 1] = snapshots[j];
                }
                snapshots[0] = null; // set the first value = null for the case in which we shift more than one time
            }
            snapshots[0] = snapshot; // save last snapshot in the history windows as the last value
            snapshot = new Snapshot(currentTime / 1000 * 1000); //refresh the current snapshot
        }
    }

}
