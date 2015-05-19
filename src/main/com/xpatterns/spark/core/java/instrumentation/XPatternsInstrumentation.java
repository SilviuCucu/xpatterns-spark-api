package com.xpatterns.spark.core.java.instrumentation;

import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;
import com.xpatterns.spark.core.java.instrumentation.metrics.XPatternsMetrics;
import com.xpatterns.spark.core.java.instrumentation.metrics.jmx.XPatternsMetricsItemMXBean;
import com.xpatterns.spark.core.java.instrumentation.metrics.jmx.XPatternsMetricsJMXManager;
import org.apache.spark.SparkEnv;

import java.io.Serializable;

/**
 * Created by radum on 15/01/15.
 */
public class XPatternsInstrumentation implements Serializable {

    private static XPatternsInstrumentation instance = null;
    String jobId = null;
    String stageId = null;
    String jobInstanceId = null;

    private static XPatternsLogger logger;
    protected XPatternsMetricsJMXManager metricsJMXManager = XPatternsMetricsJMXManager.getInstance();

    private XPatternsInstrumentation() {
        this.jobId = SparkEnv.get().conf().get("spark.xpatterns.jobId");
        this.stageId = SparkEnv.get().conf().get("spark.xpatterns.stageId");
        this.jobInstanceId = SparkEnv.get().conf().get("spark.xpatterns.jobInstanceId");
    }

    public XPatternsInstrumentation(String jobId, String stageId, String jobInstanceId) {
        this.jobId = jobId;
        this.stageId = stageId;
        this.jobInstanceId = jobInstanceId;
    }

    public static synchronized XPatternsInstrumentation getInstance() {
        if (instance == null) {
            instance = new XPatternsInstrumentation();
        }

        return instance;
    }


    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getStageId() {
        return stageId;
    }

    public void setStageId(String stageId) {
        this.stageId = stageId;
    }

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
    }

    public XPatternsLogger getLogger() {
        if (logger == null) {
            logger = new XPatternsLogger() {
                @Override
                public void debug(String message) {
                    System.out.println("DEBUG" + message);
                }

                @Override
                public void info(String message) {
                    System.out.println("INFO" + message);
                }

                @Override
                public void warn(String message) {
                    System.out.println("WARN" + message);
                }

                @Override
                public void error(String message) {
                    System.out.println("ERROR" + message);
                }

                @Override
                public void closeLogger() throws Exception {

                }
            };
        }
        return logger;
    }

    public void setLogger(XPatternsLogger log) {
        this.logger = log;
    }


    public XPatternsMetrics registerMetric(String name) {

        return metricsJMXManager.registerMetric(getInternalName(name));
    }

    public void registerMetric(String name, XPatternsMetrics xPatternsMetrics) {

        metricsJMXManager.registerMetric(getInternalName(name), xPatternsMetrics);
    }

    public XPatternsMetrics getMetric(String name) {

        return metricsJMXManager.getMetric(getInternalName(name));
    }

    public void unregisterMetric(String name) {

        metricsJMXManager.unregisterMetric(getInternalName(name));
    }

    private String getInternalName(String name) {
        return String.format(XPatternsMetricsItemMXBean.XPATTERNS_BEAN_ID, jobId, stageId, jobInstanceId, name);
    }

}
