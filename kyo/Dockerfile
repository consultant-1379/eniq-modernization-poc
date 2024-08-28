FROM armdocker.rnd.ericsson.se/proj-eson-3pp/ecson-jdk8-base:1.0.16

ADD pm-parser/target/pm-stats-parser.jar /root/ericsson/pm-stats-parser/
ADD spark-worker/target/pm-stats-parser-spark.jar /root/ericsson/pm-stats-parser-spark/
ADD pm-parser/src/main/resources/log4j.properties /usr/local/spark/conf/

ENV SPARK_LOG_LEVEL=INFO
ENV SPARK_EXECUTOR_JMX_PORT=21001
ENV PM_STATS_PARSING_SERVICE_SPARK_DRIVER_MEMORY=1G
ENV PM_STATS_PARSING_SERVICE_SPARK_PARALLELISM=12
ENV SPARK_HOME=/opt/mapr/spark/spark-2.1.0/

ADD docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
