package com.xpatterns.spark.core.java.instrumentation.metrics.jmx;

import com.xpatterns.spark.core.java.instrumentation.metrics.data.MetricsProgressItem;
import com.xpatterns.spark.core.java.instrumentation.metrics.data.MetricsStatisticsItem;

/**
 * Created by radum on 16/01/15.
 */
public interface XPatternsMetricsItemMXBean extends MetricsProgressItem, MetricsStatisticsItem {

    final String XPATTERNS_BEAN_ID = "xPatterns:app=TComponentMetrics,jobID=%s,stageID=%s,jobInstanceID=%s,name=%s";

    public String getVersion();
}