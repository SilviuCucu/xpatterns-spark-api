package com.xpatterns.spark.core.scala

import java.util.Locale

import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger
import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics}
import org.apache.spark.scheduler._

import scala.collection.JavaConversions
import scala.collection.mutable.Map

/**
 * Created by radum on 10.02.2015.
 */

object XPatternsSparkMonitoring {
  val XPATTERNS_JOBINSTANCE_ID: String = "spark.xpatterns.jobInstanceId"
  val XPATTERNS_SPARK_LOGGING_ENABLE: String = "xPatterns.spark.logging.enable"
}

class XPatternsSparkMonitoring(xLogger: XPatternsLogger) extends SparkListener {

  val SPARK_JOBID: String = "spark.jobInstanceId"
  val jobIdToUUID = Map[Integer, String]()
  val jobIdToStartTimestamp = Map[Integer, Long]()
  val stageIdToJobId = Map[Integer, Integer]()
  val stageIdToNrTasks = Map[Integer, Integer]()
  val stageIdToSuccessullTasks = Map[Integer, Integer]()
  val stageIdToShuffleRead = Map[Integer, Int]()
  val stageIdToShuffleWrite = Map[Integer, Long]()

  var isApplicationDone: Boolean = false

  override def onApplicationStart(appStart: SparkListenerApplicationStart) {
    xLogger.info("[bridge] *SparkLog* [Starting application name[" + appStart.appName + "] sparkUser[" + appStart.sparkUser + "] time[" + appStart.time + "]")
    xLogger.info("[bridge] ============================================================================================================")
  }

  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd) {
    xLogger.info("[bridge] *SparkLog* [Finished application in [" + appEnd.time + "] ===")
    xLogger.info("[bridge] ============================================================================================================")
    try {
      xLogger.info("[bridge] *SparkLog* [ clossing all loggers (Cassandra, RabbitMq) \n");
      xLogger.closeLogger();

    } catch {
      case e: Exception => xLogger.error("[bridge]  *Spark Log ERROR* [" + e + "]")
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val sparkJobId = jobStart.jobId
    val properties = jobStart.properties

    xLogger.info("[bridge] *SparkLog* [JID[" + sparkJobId + "] with SIDs[" + jobStart.stageIds.mkString(", ") + "] has been started")
    xLogger.info("[bridge] ======================================================")
    Option(properties) match {
      case None => {}
      case _ => {
        val props: Map[String, String] = JavaConversions.asScalaMap(properties)
        props.keySet.toList.sortWith(_ < _).foreach { key =>
          xLogger.info("[bridge] *SparkLog* [JID[" + sparkJobId + "] ->" + key + (" " * (30 - key.length)) + " = " + props(key))

        }


        val xPatternsJobInstanceId = props(XPatternsSparkMonitoring.XPATTERNS_JOBINSTANCE_ID) //spark.jobGroup.id")


        jobStart.properties.setProperty(SPARK_JOBID, sparkJobId.toString())

        if (!xPatternsJobInstanceId.isEmpty()) {

          jobIdToStartTimestamp.put(sparkJobId, System.currentTimeMillis())
          jobIdToUUID.put(sparkJobId, xPatternsJobInstanceId)
          try {
            // xLogger.info("[bridge] *SparkLog* [=== "+message+ " ===")

          } catch {
            case e: Exception => xLogger.error("[bridge]  *Spark Log ERROR* [" + e + "]")
          }
        }
      }
    }
  }


  override def onJobEnd(arg0: SparkListenerJobEnd) = {
    val jobId = arg0.jobId

    jobIdToUUID.get(jobId) match {
      case Some(uuid) => {
        jobIdToStartTimestamp.get(jobId) match {
          case Some(startTimestamp) => {
            val executionTime = (System.currentTimeMillis() - startTimestamp) / 1000.0
            val message = "JID[" + jobId + "] finished in " + executionTime + " s  with result " + arg0.jobResult


            isApplicationDone = true

            jobIdToUUID.remove(jobId)
            jobIdToStartTimestamp.remove(jobId)


            try {
              xLogger.info("[bridge] *SparkLog* [" + message + "]\n")
            } catch {
              case e: Exception => xLogger.error("[bridge]  *Spark Log ERROR* [" + e + "]")
            }

          }
          case None => {}
        }

      }
      case None => {}
    }

  }


  override def onStageSubmitted(stageSub: SparkListenerStageSubmitted) {
    val stageInfo: StageInfo = stageSub.stageInfo
    val stageId = stageInfo.stageId
    val properties = stageSub.properties

    Option(properties) match {
      case None => {}
      case _ => {

        val props: Map[String, String] = JavaConversions.asScalaMap(properties)
        props.keySet.toList.sortWith(_ < _).foreach { key =>
          //  xLogger.info("[bridge] *SparkLog* [SID[" + stageId + "] " + key + (" " * (30 - key.length)) + " = " + props(key))
        }

        val sparkJobId = stageSub.properties.getProperty(SPARK_JOBID)
        Option(sparkJobId) match {
          case None => {}
          case _ => {

            val jobId = Integer.parseInt(sparkJobId)

            jobIdToUUID.get(jobId) match {
              case Some(uuid) => {
                val message = "SID[" + stageId + "] was submitted for JID[" + jobId + "]"

                stageIdToJobId.put(stageId, jobId)
                stageIdToNrTasks.put(stageId, stageSub.stageInfo.numTasks)
                stageIdToSuccessullTasks.put(stageId, 0)
                stageIdToShuffleRead.put(stageId, 0)
                stageIdToShuffleWrite.put(stageId, 0)

                try {
                  xLogger.info("[bridge] *SparkLog* [SID[" + stageId + "] Name[" + stageInfo.name + "] NumberOfTasks[" + stageInfo.numTasks + "] has been started for JID[" + jobId + "]")

                } catch {
                  case e: Exception => xLogger.error("[bridge]  *Spark Log ERROR* [" + e + "]")
                }
              }
              case None => {}
            }
          }
        }
      }
    }
  }


  override def onStageCompleted(arg0: SparkListenerStageCompleted) {

    val stageId = arg0.stageInfo.stageId

    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            val executionTime = (arg0.stageInfo.completionTime.get - arg0.stageInfo.submissionTime.get) / 1000.0


            try {
              var message: String = "SID[" + stageId + "] Name[" + arg0.stageInfo.name + "] for JID[" + jobId + "] finished in " + executionTime + " s !"

              stageIdToShuffleRead.get(stageId) match {
                case None => stageIdToShuffleRead.put(stageId, 0)
                case Some(volume) => {
                  message += " Shuffle read[" + bytesToString(volume) + "]"
                }

              }
              stageIdToShuffleWrite.get(stageId) match {
                case None => stageIdToShuffleWrite.put(stageId, 0)
                case Some(volume) => {
                  message += " Shuffle write[" + bytesToString(volume) + "]"
                }

              }
              xLogger.info("[bridge] *SparkLog* [" + message + "]\n")

            } catch {
              case e: Exception => xLogger.error("[bridge]  *Spark Log ERROR* [" + e + "]")
            }

            stageIdToJobId.remove(stageId)
            stageIdToNrTasks.remove(stageId)
            stageIdToSuccessullTasks.remove(stageId)
            stageIdToShuffleRead.remove(stageId)
            stageIdToShuffleWrite.remove(stageId)
          }
          case None => {}
        }
      }
      case None => {}
    }

  }

  override def onTaskStart(arg0: SparkListenerTaskStart) {
    val taskInfo: TaskInfo = arg0.taskInfo
    val taskId = taskInfo.taskId
    val stageId = arg0.stageId


    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {

            try {
              // xLogger.debug("[bridge] *SparkLog* [Starting task SID["+stageId+"]/TID["+taskId+"/"+stageIdToNrTasks.get(stageId).getOrElse(0).asInstanceOf[Int]+"] status["+taskInfo.status+"]")


            } catch {
              case e: Exception => xLogger.error("[bridge]  *Spark Log ERROR* [" + e + "]")
            }
          }
          case None => {}
        }

      }
      case None => {}
    }

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    var taskInfo: TaskInfo = taskEnd.taskInfo
    val taskId = taskInfo.taskId
    val stageId = taskEnd.stageId



    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {

        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            var message = ""
            if (taskEnd.taskInfo.successful) {
              stageIdToSuccessullTasks.get(stageId) match {
                case Some(successfulTasks) => stageIdToSuccessullTasks.put(stageId, successfulTasks + 1)
                case None => stageIdToSuccessullTasks.put(stageId, 1)

              }
              // xLogger.info("[bridge] *SparkLog* [Finished task on SID["+stageId+"]/TID["+taskId+"] ["+taskInfo.status+"] in " + taskEnd.taskInfo.duration + " ms on " + taskEnd.taskInfo.host)
              //  xLogger.info("[bridge] *SparkLog* [===== Progress ["+  stageIdToSuccessullTasks.get(stageId).getOrElse(0) +" of " + stageIdToNrTasks.get(stageId).getOrElse(0)+" ]  =====]")


              var tasksPerStage: Integer = stageIdToNrTasks.get(stageId).get
              var succTasksPerStage: Integer = stageIdToSuccessullTasks.get(stageId).get

              var mainMesage: String = "SID[" + stageId + "] progress [ " + succTasksPerStage + " / " + tasksPerStage + " ] "
              var taskMessage: String = " - TID[" + taskId + "][" + taskInfo.status + "] in " + taskEnd.taskInfo.duration + " ms on " + taskEnd.taskInfo.host
              var shuffleMessage: String = " Shuffle [ "
              val shuffleRead: Option[ShuffleReadMetrics] = taskEnd.taskMetrics.shuffleReadMetrics
              shuffleRead.foreach(rm => {
                5
                shuffleMessage = " read[ " + bytesToString(rm.totalBlocksFetched) + " ]"
                stageIdToShuffleRead.get(stageId) match {
                  case None => stageIdToShuffleRead.put(stageId, 0)
                  case Some(readSize) => stageIdToShuffleRead.put(stageId, readSize + rm.totalBlocksFetched)
                }
              })

              val shuffleWrite: Option[ShuffleWriteMetrics] = taskEnd.taskMetrics.shuffleWriteMetrics
              shuffleWrite.foreach(wm => {

                shuffleMessage = " write[ " + bytesToString(wm.shuffleBytesWritten) + " ]"
                stageIdToShuffleWrite.get(stageId) match {
                  case None => stageIdToShuffleWrite.put(stageId, 0)
                  case Some(writeSize) => stageIdToShuffleWrite.put(stageId, writeSize + wm.shuffleBytesWritten)
                }
              })



              if (tasksPerStage <= 100) {
                xLogger.info("[bridge] *SparkLog* [" + mainMesage + taskMessage + shuffleMessage + "]")
              } else {
                val fraction: Int = tasksPerStage / 10

                val modulo: Int = succTasksPerStage % fraction
                if (0 == modulo) {
                  mainMesage = "SID[" + stageId + "] overall Tasks [ " + succTasksPerStage + " of " + tasksPerStage + " ] "
                  shuffleMessage = "overall Shuffle ["

                  stageIdToShuffleRead.get(stageId) match {
                    case None => stageIdToShuffleRead.put(stageId, 0)
                    case Some(volume) => {
                      shuffleMessage += "read [" + bytesToString(volume) + "]"
                    }

                  }
                  stageIdToShuffleWrite.get(stageId) match {
                    case None => stageIdToShuffleWrite.put(stageId, 0)
                    case Some(volume) => {
                      shuffleMessage += " write [" + bytesToString(volume) + "]"
                    }

                  }

                  xLogger.info("[bridge] *SparkLog* [" + mainMesage + shuffleMessage + "]")
                }
              }


            } else {
              xLogger.info("[bridge] *SparkLog* [Finished task on SID[" + stageId + "]/TID[" + taskId + "  has FAILED! Duration was " + taskEnd.taskInfo.duration + " ms on " + taskEnd.taskInfo.host)
            }

          }
          case None => {}
        }
      }

      case None => {}
    }

  }


  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2 * TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2 * GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2 * MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2 * KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

}

