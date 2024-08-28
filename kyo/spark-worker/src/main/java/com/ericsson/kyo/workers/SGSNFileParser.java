package com.ericsson.kyo.workers;

import static org.apache.spark.sql.functions.*;
import static com.ericsson.oss.services.sonom.common.env.Environment.getEnvironmentValue;
import static com.ericsson.oss.services.sonom.pm.stats.parsing.PmStatsParserConstants.DELIMITER_KEY;
import static com.ericsson.oss.services.sonom.pm.stats.parsing.PmStatsParserConstants.FLS_PATHS_DELIMITER;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.ReportableEntity;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.workers.spark.SGSNMapCountersUDF;

public class SGSNFileParser {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SGSNFileParser.class);
    private static final String TABLE_NAME_PATTERN = "%1$s_%2$s";
    
    private long start = System.currentTimeMillis();
    private long overalltime = System.currentTimeMillis();
    private final PostgresParserDataLoader mdr;
    private final SparkSession sparkSession;
    private final String techPackID;
    private final String flowName;
    private final String workerName;
    private final String dependencyName;

    public SGSNFileParser(final SparkSession sparkSession,
            final String techPackID, final String flowName,
            final String workerName, final String dependencyName) {
        this.techPackID = techPackID;
        this.flowName = flowName;
        this.workerName = workerName;
        this.sparkSession = sparkSession;
        this.dependencyName = dependencyName;
        mdr = new PostgresParserDataLoader();
    }

    public void execute() {
        LOGGER.info("Executing SGSNFileParser");
        
        final Properties configuration = mdr.getWorkerConfig(techPackID,
                flowName, workerName);
        final Map<String, String> outputProps = getOutputProperties();
        final List<String> list_of_files = mdr.getFileBacklog(techPackID,
                flowName, dependencyName);

        sparkSession.udf().register(SGSNMapCountersUDF.NAME, new SGSNMapCountersUDF(),
                DataTypes.createMapType(DataTypes.StringType,
                        DataTypes.StringType));

        LOGGER.info("{} files found for parsing", list_of_files.size());

        if (list_of_files.size() > 0) {
            LOGGER.info("Starting SGSNFileParser");

            String UniqueWorkerName = "SGSNFileParser";
            String processed_date = getCurrentDate();

            LOGGER.info("Loading from MDR");

            HashMap<String, ReportableEntity> persistenceEntities = mdr
                    .getPersistenceEntities(techPackID, "RAW");
            persistenceEntities = mdr.getParitionColumns(persistenceEntities,
                    techPackID, "RAW");

            LOGGER.info("Parsing files");
            Dataset<Row> sourceData = sparkSession.read().format("xml-file-v2")
                    .option("rowTag", configuration.getProperty("rowTag"))
                    .option(DELIMITER_KEY, FLS_PATHS_DELIMITER)
                    .load(String.join(FLS_PATHS_DELIMITER, list_of_files));
            
            final Dataset<Row> pmDataFrame2 = sourceData.select(
                    col("measData.managedElement._localDn").as("node"), 
                    col("measData.managedElement.measInfo.granPeriod._endTime").as("ropendtime"),
                    col("measData.managedElement.measInfo.granPeriod._duration").as("granularity"), 
                    col("measData.managedElement.measInfo.measType"),
                    col("measData.managedElement.measInfo._measInfoId").as("measurement"),
                    col("measData.managedElement.measInfo.measValue._measObjLdn").as("moid"),
                    col("measData.managedElement.measInfo.measValue.r").as("mvList"));
            
            final Dataset<Row> pmDataFrame3 = pmDataFrame2.withColumn("ropendtime", to_utc_timestamp((col("ropendtime")), "yyyy-MM-dd HH:mm:ss"));
            
            final Dataset<Row> pmDataFrame4 = pmDataFrame3.withColumn("local_timestamp", unix_timestamp(col("ropendtime"), "yyyy-MM-dd HH:mm:ss"));
            
            final Dataset<Row> pmDataFrame5 = pmDataFrame4.withColumn("date_id", to_date(col("ropendtime")));

            final Dataset<Row> pmDataFrame6 = pmDataFrame5.select(
                    col("ropendtime"), col("date_id"), col("node"),
                    expr("substring(granularity, 3, 3)").as("granularity"),
                    col("moid"), col("measurement"),
                    callUDF(SGSNMapCountersUDF.NAME, col("measType"), col("mvList"))
                            .alias("counterMap"));

            final Dataset<Row> parsedData = pmDataFrame6.select(
                    col("ropendtime"), col("date_id"), 
                    col("granularity"), col("moid"),
                    col("node"), col("measurement"),
                    explode(col("counterMap")).as(
                            new String[] { "counter_name", "counter_value_raw" }))
                    .drop("counterMap");
            final Dataset<Row> parsedDataWithoutPDF = parsedData.withColumn("counter_value",
                    expr("cast(substring_index(counter_value_raw,',',1)as long)"))
                    .withColumn("counter_name", expr("lower(counter_name_raw)"))
                    .drop("counter_value_raw", "counter_name_raw");
            
            parsedDataWithoutPDF.createOrReplaceTempView(UniqueWorkerName);
            parsedDataWithoutPDF.cache();
            parsedData.show(false);
            
            LOGGER.info("Stage 3: {}", getElapsedTime());

            LOGGER.info("Parsing data for {} persistence entities",
                    persistenceEntities.size());
            for (Map.Entry<String, ReportableEntity> entry : persistenceEntities.entrySet()) {
                String meas = entry.getKey();
                ReportableEntity entity = entry.getValue();


                String sql = "SELECT "
                        + mergeColumnsQuery(entity.getColumns().get("key"))
                        + ", measurement, counter_name, counter_value FROM "
                        + UniqueWorkerName + " where measurement = '"
                        + entity.getMOname() + "' and counter_name in ("
                        + mergeColumns(entity.getColumns().get("counter"))
                        + ")";

                Dataset<Row> data = sparkSession.sql(sql);

                LOGGER.info("{} : {} query", getTotalElapsedTime(),
                        getElapsedTime(), meas);


                ArrayList<String> paritioning = entity.getParitionby();

                data = data
                        .groupBy("granularity", "moid", "nedn", "node",
                                "ropendtime", "date_id", "measurement")
                        .pivot("counter_name",
                                Arrays.asList(entity.getColumns().get("counter")
                                        .toArray()))
                        .sum("counter_value")
                        .drop("measurement", "counter_name");

                LOGGER.info("{} : {} {} pivot", getTotalElapsedTime(),
                        getElapsedTime(), meas);

                outputProps.put("dbtable",
                        String.format(TABLE_NAME_PATTERN, techPackID, meas));

                data.write().format("jdbc")
                        .partitionBy(
                                paritioning.stream().toArray(String[]::new))
                        .mode(SaveMode.Append).options(outputProps).save();

                LOGGER.info("{} : {} {} file write", getTotalElapsedTime(),
                        getElapsedTime(), meas);

            }
            
            
            mdr.updateFileBacklog(techPackID, flowName,  mergeColumns(list_of_files), processed_date, "Parsed");
        }

        LOGGER.info("{} : SGSNFileParser Complete", getTotalElapsedTime());
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

    public String mergeColumns(List<String> columns) {
        final String sql = String.join("','", columns);
        return sql.length() > 0 ? "'" + sql + "'" : "";
    }

    public boolean isDatasetEmpty(Dataset<Row> ds) {
        boolean isEmpty;
        try {
            ds.first();
            isEmpty = ((Row[]) ds.head(1)).length == 0;
        } catch (Exception e) {
            return true;
        }
        return isEmpty;
    }
    
    private String getCurrentDate() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        return dateformat.format(cal.getTime());
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
    
    private static Map<String, String> getOutputProperties() {
        final Map<String, String> outputProps = new HashMap<>();
        outputProps.put("url", getEnvironmentValue("HSJDBCConnection",
                "jdbc:postgresql://localhost:5432/kyo_hs"));
        outputProps.put("user", getEnvironmentValue("HSWriteUser", "hs_write"));
        outputProps.put("password",
                getEnvironmentValue("HSWritePassword", "hs_write"));
        outputProps.put("driver",
                getEnvironmentValue("HSDriver", "org.postgresql.Driver"));
        return outputProps;
    }
    

}
