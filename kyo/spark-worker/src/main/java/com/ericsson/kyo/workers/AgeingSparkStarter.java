package com.ericsson.kyo.workers;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgeingSparkStarter {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkStarter.class);

    public static void main(String[] args) {

        final SparkSession sparkSession = SparkSession.builder().getOrCreate();
        final String techpackId = args[1];
        final String flowName = args[2];
        final String workerName = args[3];
        final String dependencyName = args[4];
        final DataAgeing ageing = new DataAgeing(sparkSession, techpackId,
                flowName, workerName, dependencyName);
        ageing.execute();
    }
}
