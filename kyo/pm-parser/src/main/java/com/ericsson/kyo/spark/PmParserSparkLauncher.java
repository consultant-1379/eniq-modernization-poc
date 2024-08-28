/*
 * *------------------------------------------------------------------------------
 * ******************************************************************************
 *  COPYRIGHT Ericsson 2018 - 2019
 *
 *  The copyright to the computer program(s) herein is the property of
 *  Ericsson Inc. The programs may be used and/or copied only with written
 *  permission from Ericsson Inc. or in accordance with the terms and
 *  conditions stipulated in the agreement/contract under which the
 *  program(s) have been supplied.
 * ******************************************************************************
 * ------------------------------------------------------------------------------
 */
package com.ericsson.kyo.spark;

import static com.ericsson.kyo.spark.PmParserSparkLauncherConfiguration.*;
import static com.ericsson.oss.services.sonom.common.env.Environment.getEnvironmentValue;

import java.time.Duration;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.oss.services.sonom.common.spark.launcher.AbstractSparkLauncher;

import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

/**
 * Class which is used to launch a Spark job for parsing PM counters for all
 * node types.
 * <p>
 * Uses the class <code>com.ericsson.kyo.workers.SparkStarter</code> in the
 * <code>pm-stats-parser-spark</code> JAR.
 */
public class PmParserSparkLauncher extends AbstractSparkLauncher {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(PmParserSparkLauncher.class);

    private static final String PM_PARSER_JARS_DIRECTORY_PATH = getEnvironmentValue(
            "user.home", "/") + "/ericsson/pm-stats-parser-spark/";
    private static final String SPARK_MASTER_URL = getEnvironmentValue(
            "PM_STATS_PARSING_SERVICE_SPARK_MASTER_URL");
    private static final String SPARK_LOG_LEVEL = getEnvironmentValue(
            "SPARK_LOG_LEVEL");
    private static final String EXECUTOR_JMX_PORT = getEnvironmentValue(
            "SPARK_EXECUTOR_JMX_PORT");
    private static final String SPARK_HOME = getEnvironmentValue("SPARK_HOME",
            "/opt/spark/");
    private static final String APPLICATION_ID = "PM stats processor";

    private String ossId;
    private String techpackId;
    private String flowName;
    private String workerName;
    private String dependencyName;
    private String sparkMainClass;

    public void launchParser(final String ossId, final String techpackId,
            final String flowName, final String workerName,
            final String dependencyName, final String SparkLauncherMainClass) {
        setOssId(ossId);
        this.techpackId = techpackId;
        this.flowName = flowName;
        this.workerName = workerName;
        this.dependencyName = dependencyName;
        this.sparkMainClass = SparkLauncherMainClass;
        setApplicationHomeDirectory(PM_PARSER_JARS_DIRECTORY_PATH);
        startSparkLauncher(APPLICATION_ID);
    }

    @Override
    protected SparkLauncher getSparkLauncher() {
        LOGGER.info("Launching PM stats parsing configuration tasks");

        final String driverExtraJavaOptions = "-Dhadoop.login=simple "
                + "-Dzookeeper.sasl.clientconfig=Client_simple "
                + "-Dlog4j.configuration=file:/usr/local/spark/conf/log4j.properties -Dlog.level="
                + SPARK_LOG_LEVEL;
        final String executorJmxOptions = "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=" + EXECUTOR_JMX_PORT +
                " -Dcom.sun.management.jmxremote.rmi.port=" + EXECUTOR_JMX_PORT +
                " -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false";
        final String executorExtraJavaOptions = "-XX:+UseG1GC -XX:G1HeapRegionSize=2M -XX:MaxGCPauseMillis=1000 "
                + executorJmxOptions + " -XX:+UseStringDeduplication -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/executor_heap.hprof";

        final String parallelism = getEnvironmentValue(
                "PM_STATS_PARSING_SERVICE_SPARK_PARALLELISM", "2");

        final String executors = "" + Integer.parseInt(parallelism) / 4;

        return new SparkLauncher()
                .setAppResource(PM_PARSER_JARS_DIRECTORY_PATH
                        + "pm-stats-parser-spark.jar")
                .setMainClass(sparkMainClass).setMaster(SPARK_MASTER_URL)
                .setSparkHome(SPARK_HOME)
                .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
                        driverExtraJavaOptions)
                .setConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS,
                        executorExtraJavaOptions)
                .setConf(SparkLauncher.DRIVER_MEMORY,
                        getEnvironmentValue(
                                "PM_STATS_PARSING_SERVICE_SPARK_DRIVER_MEMORY"))
                .setConf(SparkLauncher.EXECUTOR_MEMORY, getEnvironmentValue(
                        "PM_STATS_PARSING_SERVICE_SPARK_EXECUTOR_MEMORY"))
                .setConf("spark.driver.host",
                        getEnvironmentValue(
                                "PM_STATS_PARSING_SERVICE_SPARK_DRIVER_HOST"))
                .setConf("spark.serializer",
                        "org.apache.spark.serializer.KryoSerializer")
                .setConf("spark.metrics.namespace", techpackId + "-" + workerName)
                .setConf("spark.default.parallelism", parallelism)
                .setConf("spark.sql.shuffle.partitions", parallelism)
                .setConf("spark.eventLog.enabled", "false")
                .setConf("spark.executor.instances", executors)
                .setConf("spark.task.maxFailures", "1")
                .setConf("spark.executor.cores", "4").addAppArgs(ossId,
                        techpackId, flowName, workerName, dependencyName);
    }

    @Override
    protected Retry getRetry() {
        final RetryConfig config = RetryConfig.<SparkAppHandle.State> custom()
                .retryOnResult(SPARK_JOB_FAILED_STATES::contains)
                .maxAttempts(MAX_SPARK_RETRY_ATTEMPTS)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(
                        Duration.ofMillis(SPARK_RETRY_DURATION_VALUE_MS),
                        SPARK_RETRY_MULTIPLIER))
                .retryExceptions(Throwable.class).build();
        return Retry.of("PmStatsSparkApplicationRetry", config);
    }

    private void setOssId(final String ossId) {
        this.ossId = ossId;
    }
}