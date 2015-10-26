__author__ = 'silviuc'

from py4j.java_gateway import JavaGateway, GatewayClient, java_import
from pyspark import SparkContext, SparkConf


class XPatternsContextProvider:
    def __init__(self, host, port):
        # Connect to the py4j GatewayServer provided by the PythonJobLauncher to gain access
        # to objects related to the Spark context, in the bridge JVM
        gateway_parameters = GatewayClient(address=host, port=port)
        gateway = JavaGateway(gateway_parameters)

        # Import the classes used by PySpark, to prevent "py4j.protocol.Py4JError: Trying to call a package."
        # exceptions in the pyspark code, when accessing jvm classes via the gateway
        java_import(gateway.jvm, "org.apache.spark.SparkConf")
        java_import(gateway.jvm, "org.apache.spark.api.java.*")
        java_import(gateway.jvm, "org.apache.spark.api.python.*")
        java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
        java_import(gateway.jvm, "org.apache.spark.sql.*")
        java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
        java_import(gateway.jvm, "scala.Tuple2")

        # Create a Python SparkConf and SparkContext based on the Java ones provided via the gateway
        java_conf = gateway.entry_point.getJavaSparkConf()
        java_context = gateway.entry_point.getJavaSparkContext()

        self._python_conf = SparkConf(_jconf=java_conf)
        self._python_context = SparkContext(jsc=java_context, gateway=gateway, conf=self._python_conf)
        # Distribute the instrumentation provider file to the worker nodes
        self._python_context.addPyFile(gateway.entry_point.getXPatternsInstrumentationProviderFilePath())

        # Store the other relevant Java objects accessible via the gateway
        self._spark_job_config = gateway.entry_point.getSparkJobConfig()
        self._instrumentation = gateway.entry_point.getXPatternsInstrumentation()
        self._logger = gateway.entry_point.getXPatternsLogger()

    def get_xpatterns_spark_context(self):
        return self._python_context

    def get_xpatterns_spark_conf(self):
        return self._python_conf

    def get_xpatterns_spark_job_config(self):
        return self._spark_job_config

    def get_xpatterns_logger(self):
        return self._logger

    # This functionality will be made available in the future
    # def get_xpatterns_instrumentation(self):
    #     return self._instrumentation
