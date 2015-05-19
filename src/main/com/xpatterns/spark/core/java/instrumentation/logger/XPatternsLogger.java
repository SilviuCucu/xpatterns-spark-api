package com.xpatterns.spark.core.java.instrumentation.logger;

import java.io.Serializable;


/**
 * Created by radum on 10.04.2014.
 */

public interface XPatternsLogger extends Serializable {
    public void debug(String message);

    public void info(String message);

    public void warn(String message);

    public void error(String message);

    public void closeLogger() throws Exception;
}
