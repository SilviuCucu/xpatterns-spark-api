__author__ = 'silviuc'

from py4j.java_gateway import JavaGateway, GatewayClient, java_import
from pyspark import SparkContext, SparkConf


class XPatternsContextProvider:
    """ Class meant for users that want to run Python Spark jobs via a XPatterns TComponent PySpark stage.
    Provides access to an enhanced Python SparkContext backed by a JavaSparkContext, whose lifecycle is managed
    via Atigeo SparkJobRest server. The JavaSparkContext has a XPatterns SparkJobListener attached to it,
    which allows the user to monitor the Spark job's progress in the corresponding TComponent stage logs.
    This class also provides access to context and current job related entities. """

    def get_xpatterns_spark_context(self):
        """ Provide the enhanced SparkContext """
        return self.__instance.get_xpatterns_spark_context()

    def get_xpatterns_spark_conf(self):
        """ Provide the Spark configuration used for the enhanced SparkContext """
        return self.__instance.get_xpatterns_spark_conf()

    def get_xpatterns_spark_job_config(self):
        """ Provide the configuration of the current Spark job as a wrapper of Java com.typesafe.config.Config """
        return self.__instance.get_xpatterns_spark_job_config()

    def get_xpatterns_logger(self):
        """ Provide a logger that sends the output to the current TComponent stage logs """
        return self.__instance.get_xpatterns_logger()

    def shutdown(self):
        """ Should be called once the enhanced SparkContext is no longer needed, to ensure cleanup """
        self.__instance.shutdown_gateway()

    class __ContextProvider:
        """ This nested class ensures there is only one instance of the context provider.
        The nested class implementation should be considered private by users of the XPatternsContextProvider class """

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

        def __str__(self):
            return repr(self)

        def get_xpatterns_spark_context(self):
            return self._python_context

        def get_xpatterns_spark_conf(self):
            return self._python_conf

        def get_xpatterns_spark_job_config(self):
            return self._spark_job_config

        def get_xpatterns_logger(self):
            return self._logger

        def shutdown_gateway(self):
            self._gateway.shutdown()

    # storage for the instance reference
    __instance = None

    def __init__(self, argv=None):
        """ Create singleton instance """
        # Check whether we already have an instance
        if XPatternsContextProvider.__instance is None:
            if argv is None:
                error_message = ("The Py4J GatewayServer host and the port it is listening on are required "
                                 "to properly instantiate the singleton instance of the " + self.__class__.__name__ +
                                 " class. They are the last two elements of the argv parameter.")
                print error_message
                raise Exception(error_message)
            else:
                if len(argv) > 2:
                    # Py4J GatewayServer host and the port it is listening on
                    host = argv[-2]
                    port = int(argv[-1])

                    # Create and remember instance
                    XPatternsContextProvider.__instance = XPatternsContextProvider.__ContextProvider(host=host,
                                                                                                     port=port)
                else:
                    raise Exception("The arguments list provided to the Python script does not contain "
                                    "the host and port parameters required to connect to the Py4J GatewayServer")

    def __str__(self):
        return repr(XPatternsContextProvider.__instance)
