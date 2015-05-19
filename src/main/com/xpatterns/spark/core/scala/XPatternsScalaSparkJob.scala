package com.xpatterns.spark.core.scala

import api.SparkJobValidation
import com.typesafe.config.Config
import org.apache.spark.SparkContext

/**
 * Created by radum on 10.04.2014.
 */
trait XPatternsScalaSparkJob {

  def runJob(sc: SparkContext, jobConfig: Config): java.io.Serializable;

  def validate(sc: SparkContext, config: Config): SparkJobValidation;
}