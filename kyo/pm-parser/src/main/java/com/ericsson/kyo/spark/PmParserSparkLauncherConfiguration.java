/*
 * *------------------------------------------------------------------------------
 * ******************************************************************************
 *  COPYRIGHT Ericsson 2019
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

import static com.ericsson.oss.services.sonom.common.env.Environment.getEnvironmentValue;
import static com.ericsson.oss.services.sonom.common.spark.launcher.SparkLauncherConfiguration.convertToMilliSeconds;

import java.util.concurrent.TimeUnit;

/**
 * Holds configuration values for the pm stats transformer spark launcher.
 */
class PmParserSparkLauncherConfiguration {

    static final String SPARK_RETRY_DURATION_TIME_UNIT = getEnvironmentValue("PM_STATS_PARSING_SERVICE_SPARK_RETRY_DURATION_TIME_UNIT",
            TimeUnit.MINUTES.toString());

    static final String SPARK_RETRY_DURATION = getEnvironmentValue("PM_STATS_PARSING_SERVICE_SPARK_RETRY_DURATION", "1");

    static final Integer MAX_SPARK_RETRY_ATTEMPTS = Integer
            .parseInt(getEnvironmentValue("PM_STATS_PARSING_SERVICE_MAX_SPARK_RETRY_ATTEMPTS", "1"));

    static final Double SPARK_RETRY_MULTIPLIER = Double
            .parseDouble(getEnvironmentValue("PM_STATS_PARSING_SERVICE_SPARK_RETRY_MULTIPLIER", "1.5"));

    static final Long SPARK_RETRY_DURATION_VALUE_MS = convertToMilliSeconds(SPARK_RETRY_DURATION_TIME_UNIT, SPARK_RETRY_DURATION);
}
