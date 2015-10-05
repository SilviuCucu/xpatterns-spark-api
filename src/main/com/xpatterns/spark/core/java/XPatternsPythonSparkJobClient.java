package com.xpatterns.spark.core.java;

import java.util.Date;
import java.util.HashSet;


/**
 * Created by radum on 10.04.2014.
 */

public abstract class XPatternsPythonSparkJobClient extends XPatternsSparkJobClient {
    private String XPATTERNS_SJS_BRIDGE_CLASS_PATH = "com.spark.job.server.scala.XPatternsPythonSparkBridge";


    public XPatternsPythonSparkJobClient(String uri) {
        super(uri);
    }

    public String getXPatternsSjsBridgeClassPath() {
        return XPATTERNS_SJS_BRIDGE_CLASS_PATH;
    }

    public String launchXPatternsSparkJob(String mainPythonFile, String resources, HashSet<String> parameters, String context, Boolean sync) throws Exception {
        StringBuffer input = new StringBuffer("mainPythonFile=" + mainPythonFile + "\n resources=\"" + resources + "\"");
        if (parameters != null) {
            input.append("\n");
            for (String pair : parameters) {
                input.append(pair);
                input.append("\n");
            }
        }

        input.append("xpatterns_submission_date=" + CUSTOM_DATE.format(new Date()) + "\n");

        return launchJob(input.toString(), context, sync);
    }
}
