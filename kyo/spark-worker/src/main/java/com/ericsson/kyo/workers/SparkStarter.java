package com.ericsson.kyo.workers;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.workers.spark.VerticaDialect;

public class SparkStarter {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkStarter.class);

    public static void main(String[] args) {
    	
        final SparkSession sparkSession = SparkSession.builder().getOrCreate();
        //Workaround for String to VARCHAR conversion on Vertica writes
        JdbcDialects.registerDialect(new VerticaDialect());
        final String techpackId = args[1];
        final String flowName = args[2];
        final String workerName = args[3];
        final String dependencyName = args[4];
        
        final PMFileParser parser = new PMFileParser(sparkSession, techpackId,
                flowName, workerName, dependencyName);
        parser.execute();
    }
}
