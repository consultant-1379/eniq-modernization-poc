package com.ericsson.kyo.workers;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

import com.ericsson.kyo.workers.spark.VerticaDialect;

public class AltSparkStarter {

	public static void main(String[] args) {
		//Set Environmental Variables
		System.setProperty("hadoop.home.dir", "C:/Users/ebrifol/Dev/Aidos/HadoopBinary");
		
		System.setProperty("MDRJDBCConnection", "jdbc:postgresql://ieatrcx7673-5.athtem.eei.ericsson.se:30828/kyo_mdr");
		System.setProperty("MDRUser", "custom-postgres");
		System.setProperty("MDRPassword", "kFs8wlg1jA");
		
//		System.setProperty("MDRJDBCConnection", "jdbc:postgresql://localhost:5432/KYO_postgres");
//		System.setProperty("MDRUser", "KYO_dba");
//		System.setProperty("MDRPassword", "dba123");
		
		
		final SparkSession sparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("Tester")
				.config("spark.driver.port", 50000)
				.config("spark.driver.host", "localhost")
				.getOrCreate();
        //Workaround for String to VARCHAR conversion on Vertica writes
        JdbcDialects.registerDialect(new VerticaDialect());
        final String techpackId = "PM_E_ERBS_R28M01";
        final String flowName = "PM_E_ERBS";
        final String workerName = "com.ericsson.kyo.workers.SparkLauncherWorker";
        final String dependencyName = "PM_E_ERBS";
        
        final AltPmFileParser parser = new AltPmFileParser(sparkSession, techpackId,
                flowName, workerName, dependencyName);
        parser.execute();
        
	}

}
