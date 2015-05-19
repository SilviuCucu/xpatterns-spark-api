package com.xpatterns.spark.core.java;

import com.typesafe.config.Config;
import com.xpatterns.spark.core.java.validation.XPatternsJobValidation;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by radum on 10.04.2014.
 */


public interface XPatternsJavaSparkJob {

    public XPatternsJobValidation validate(JavaSparkContext javaSparkContext, Config configuration);

    public Serializable run(JavaSparkContext javaSparkContext, Config configuration);

}
