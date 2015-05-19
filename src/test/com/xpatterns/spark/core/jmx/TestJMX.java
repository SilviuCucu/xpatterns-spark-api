package com.xpatterns.spark.core.jmx;

import com.xpatterns.spark.core.java.instrumentation.XPatternsInstrumentation;
import com.xpatterns.spark.core.java.instrumentation.metrics.XPatternsMetrics;

/**
 * Created by radum on 16/01/15.
 */
public class TestJMX {
    public static void main(String[] args) throws InterruptedException {

        for (int j = 0; j < 100; j++) {
            XPatternsInstrumentation xpi = new XPatternsInstrumentation("1", "" + j, "3");
            XPatternsMetrics metrics = xpi.registerMetric("increment");

            for (int i = 0; i < 100; i++) {
                Thread.sleep(100);
                metrics.incrementProcessedItemsBy(10, 123456);

                System.out.println("i:" + i + " getTotal:" + metrics.getTotalProcessedItems() + " getCalls:" + metrics.getTotalCalls());
            }
        }
    }
}
