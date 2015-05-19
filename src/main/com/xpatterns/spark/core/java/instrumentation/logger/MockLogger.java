package com.xpatterns.spark.core.java.instrumentation.logger;

/**
 * Created by cosmin on 26.06.2014.
 */
public class MockLogger implements XPatternsLogger {
    public void debug(String message) {
        System.out.println("DEBUG:" + message);
    }

    public void info(String message) {
        System.out.println("INFO:" + message);
    }

    public void warn(String message) {
        System.out.println("WARN:" + message);
    }

    public void error(String message) {
        System.out.println("ERROR:" + message);
    }

    public void closeLogger() throws Exception {
        System.out.println("Close Logger called");
    }
}
