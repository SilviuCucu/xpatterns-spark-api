package com.xpatterns.spark.core.java.entity.util;

/**
 * Created by alinb on 24.09.2014.
 */

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;

import java.util.Map;

public class SparkUtil {

    public static void printAllParams(Config configuration, final XPatternsLogger log) {
        StringBuffer spark = new StringBuffer();
        StringBuffer nonSpark = new StringBuffer();

        for (Map.Entry<String, ConfigValue> entry : configuration.entrySet()) {
            String key = entry.getKey();
            ConfigValue value = entry.getValue();
            if (key.startsWith("spark")) {
                spark.append("\n[bridge]    SPARK Key:" + key + " value:" + value.toString());
            } else {
                nonSpark.append("\n[bridge] nonSPARK Key:" + key + " value:" + value.toString());

            }
        }

        log.info("***********************************************************************************************");
        log.info(spark.toString());
        log.info("***********************************************************************************************");
        log.info(nonSpark.toString());
        log.info("***********************************************************************************************");
    }
}
