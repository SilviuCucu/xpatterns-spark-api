package com.xpatterns.spark.core.java.instrumentation.metrics.jmx;

import com.xpatterns.spark.core.java.instrumentation.metrics.XPatternsMetrics;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by radum on 15/01/15.
 */
public class XPatternsMetricsJMXManager implements Serializable {

    private static Logger logger = Logger.getLogger(XPatternsMetricsJMXManager.class);
    private static final Map<String, XPatternsMetrics> beans = new ConcurrentHashMap();
    private static XPatternsMetricsJMXManager instance = null;

    private XPatternsMetricsJMXManager() {

    }

    public synchronized final static XPatternsMetricsJMXManager getInstance() {
        if (instance == null) {
            instance = new XPatternsMetricsJMXManager();
        }

        return instance;
    }

    public synchronized XPatternsMetrics registerMetric(String name) {

        XPatternsMetrics metrics = getMetric(name);
        if (metrics == null) {
            System.out.println("********Registering MXBean****: " + name);
            logger.info("********Registering MXBean******: " + name);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            metrics = new XPatternsMetrics();

            ObjectName objectName = null;
            try {
                objectName = new ObjectName(name);
            } catch (Exception e) {
                logger.warn(e.getMessage());
            }

            try {
                mbs.registerMBean(metrics, objectName);
            } catch (Exception e) {
                logger.warn(e.getMessage());
            }

            beans.put(name, metrics);
        }
        return metrics;
    }

    public synchronized void registerMetric(String name, XPatternsMetrics xPatternsMetric) {


        logger.info("Registering MXBean: " + name);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ObjectName objectName = null;
        try {
            objectName = new ObjectName(name);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }

        try {
            mbs.registerMBean(xPatternsMetric, objectName);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }


    public XPatternsMetrics getMetric(String name) {
        return beans.get(name);
    }

    public void unregisterMetric(String name) {
        beans.remove(name);
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(name));
            logger.info("Unregistering MXBean: " + name);
        } catch (Exception e) {
            logger.warn("Could not unregister MBean " + ExceptionUtils.getStackTrace(e));
        }
    }

}