package com.ericsson.kyo.workers;

import static org.apache.spark.sql.functions.*;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.impala.ImpalaRunSQL;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.mdr.ReportableEntity;
import com.ericsson.kyo.postgres.PostgresHotStore;

public class DataAgeing {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(DataAgeing.class);
    private static final String TABLE_NAME_PATTERN = "%1$s_%2$s";
    private final ImpalaRunSQL impala = new ImpalaRunSQL();
    private final PostgresHotStore hotstore = new PostgresHotStore();
    private Properties HSJdbcProperties;
    private String HSJdbcConnection;
    private long start = System.currentTimeMillis();
    private long overalltime = System.currentTimeMillis();
    private final PostgresParserDataLoader mdr;
    private final SparkSession sparkSession;
    private final String techPackID;
    private final String flowName;
    private final String workerName;
    private final String dependencyName;

    public DataAgeing(final SparkSession sparkSession, final String techPackID,
            final String flowName, final String workerName,
            final String dependencyName) {
        this.techPackID = techPackID;
        this.flowName = flowName;
        this.workerName = workerName;
        this.sparkSession = sparkSession;
        this.dependencyName = dependencyName;
        mdr = new PostgresParserDataLoader();
    }

    public void execute() {
        LOGGER.info("Executing DataAgeing");

        HSJdbcConnection = hotstore.getConnectionURL("READ");
        HSJdbcProperties = hotstore.getConnectionProperties("READ");

        LOGGER.info("Loading HSV details from MDR");
        HashMap<String, ReportableEntity> HSVEntities = mdr
                .getPersistenceEntities(techPackID, "RAW");
        HSVEntities = mdr.getParitionColumns(HSVEntities, techPackID, "RAW");

        LOGGER.info("Ageing data out to DFS");
        transferToParquet(HSVEntities);

        LOGGER.info("Ageing file backlog out to DFS");
        //transferFileBacklog();
    }

    public void transferToParquet(
            HashMap<String, ReportableEntity> ViewEntities) {

        final Properties configuration = mdr.getWorkerConfig(techPackID,
                flowName, workerName);
        for (Map.Entry<String, ReportableEntity> entry : ViewEntities
                .entrySet()) {            
            Integer retention = 3;
            Integer maxRetention = 5;
            final String meas = entry.getKey();
            LOGGER.info("Checking rows to archive for {}", meas);
            final ReportableEntity entity = entry.getValue();
            final String inputTable = String.format(TABLE_NAME_PATTERN,
                    techPackID, meas);

            final String tableQuery = "(select ('x'||substr(md5(nedn),1,8))::bit(32)::int as hash,* from "
                    + inputTable + ") as " + inputTable + "_view";

            String sql = "SELECT date_id, ropendtime from " + inputTable
                    + " WHERE ropendtime <= CAST((current_date - interval '"
                    + retention + "' days) AS TIMESTAMP)"
                    + " AND ropendtime >= CAST((current_date - interval '"
                    + maxRetention + "' days) AS TIMESTAMP)";

            final Dataset<Row> data = sparkSession.read().format("jdbc")
                    .option("url", HSJdbcConnection)
                    .option("user", HSJdbcProperties.getProperty("user"))
                    .option("password",
                            HSJdbcProperties.getProperty("password"))
                    .option("dbtable", tableQuery)
                    .option("partitionColumn", "hash")
                    .option("lowerBound", Integer.MIN_VALUE)
                    .option("upperBound", Integer.MAX_VALUE)
                    .option("numPartitions", 200)
                    .option("driver", HSJdbcProperties.getProperty("driver"))
                    .load();
            
            data.drop("hash").createOrReplaceTempView(inputTable);

            final Dataset<Row> timestamps = sparkSession.sql(sql)
                    .select("date_id").distinct().sort("date_id").limit(1);
            
            final List<String> datesList = timestamps
                    .map(r -> r.getString(0), Encoders.STRING())
                    .collectAsList();

            final List<String> columns = new ArrayList<>(
                    entity.getColumns().get("key"));
            columns.addAll(entity.getColumns().get("counter"));

            final Dataset<Row> archived = data
                    .filter(f -> datesList.contains(f.getAs("date_id")))
                    .selectExpr(columns.stream().toArray(String[]::new));

            LOGGER.info("Found {} rows to archive", archived.count());

            final ArrayList<String> paritioning = entity.getParitionby();

            final String outputPath = configuration.getProperty("OutputPath")
                    + "/" + meas;

            archived.write()
                    .partitionBy(paritioning.stream().toArray(String[]::new))
                    .mode(SaveMode.Append).parquet(outputPath);

            timestamps.foreachPartition(new DataDeletionFunction(
                    HSJdbcConnection, HSJdbcProperties, inputTable));

            LOGGER.info("{} : {} {} parquet write", getTotalElapsedTime(),
                    getElapsedTime(), inputTable);
            
            removeOldData(entity.getMappedMOs(), retention);
            LOGGER.info("{} : {} {} Cold Store delete", getTotalElapsedTime(),
                    getElapsedTime(), inputTable);

            LOGGER.info("{} : {} {} Updating Impala", getTotalElapsedTime(),
                    getElapsedTime(), inputTable);

            LOGGER.info("{} : {} query", getTotalElapsedTime(),
                    getElapsedTime(), inputTable);
        }
    }

    public void refreshImpala(String tablename, Set<String> datesMoved) {
        for (String date : datesMoved) {
            impala.executeSQL("REFRESH " + tablename + " PARTITION (date_id="
                    + date + ")");
        }
    }

    public void transferFileBacklog() {

        String sql = "SELECT *" + " FROM FILEBACKLOG"
                + " WHERE RECEIVED_DATE <= CAST((current_date - interval '3 days') AS TIMESTAMP);";

        Dataset<Row> data = sparkSession.read().format("jdbc")
                .option("url", HSJdbcConnection).option("query", sql)
                .option("user", HSJdbcProperties.getProperty("user"))
                .option("password", HSJdbcProperties.getProperty("password"))
                .load();
        data.cache();

        data.write().partitionBy("RECEIVED_DATE").mode(SaveMode.Append)
                .parquet("maprfs:///PM/SYS/FILEBACKLOG");

        mdr.deleteOldFileBacklog("3");
    }

    public void removeOldData(ArrayList<String> HSTTables, Integer retention) {
        for (String tablename : HSTTables) {
            String sql = "DELETE FROM " + tablename
                    + " WHERE ropendtime <= CAST((current_date - interval '"
                    + retention + " days') AS TIMESTAMP)";

            /**
             * Spark would be the better approach here to delete. Not seeing how
             * the HST tables come together with Spark. Are they internally
             * inferred or will Spark see the X tables as individual entities
             * like different MO tables.
             */
        }
    }

    public String mergeColumnsQuery(List<String> columns) {
        String sql = "";
        for (String column : columns) {
            if (!sql.equals("")) {
                sql = sql + ", ";
            }

            sql = sql + column;
        }
        return sql;
    }

    private long getElapsedTime() {
        long now = System.currentTimeMillis();
        long elapsedtime = now - start;
        start = now;

        return elapsedtime / 1000;
    }

    private long getTotalElapsedTime() {
        long now = System.currentTimeMillis();
        long elapsedtime = now - overalltime;

        return elapsedtime / 1000;
    }
}
