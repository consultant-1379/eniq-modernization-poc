package com.ericsson.kyo.workers;

import static org.apache.spark.sql.functions.*;
import static com.ericsson.oss.services.sonom.common.env.Environment.getEnvironmentValue;
import static com.ericsson.oss.services.sonom.pm.stats.parsing.PmStatsParserConstants.DELIMITER_KEY;
import static com.ericsson.oss.services.sonom.pm.stats.parsing.PmStatsParserConstants.FLS_PATHS_DELIMITER;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.ReportableEntity;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.workers.spark.MapCountersUDF;
import com.ericsson.kyo.workers.spark.ParseISOTimeUDF;

public class PMFileParser {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(PMFileParser.class);
    private static final String TABLE_NAME_PATTERN = "%1$s_%2$s";

    private final PostgresParserDataLoader mdr;
    private final SparkSession sparkSession;
    private final String techPackID;
    private final String flowName;
    private final String workerName;
    private final String dependencyName;

    public PMFileParser(final SparkSession sparkSession,
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
        final long parseStartTime = System.currentTimeMillis();

        LOGGER.info("Executing PMFileParser");

        final Properties configuration = mdr.getWorkerConfig(techPackID,
                flowName, workerName);
        final Map<String, String> outputProps = getOutputProperties();
        final List<String> list_of_files = mdr.getFileBacklog(techPackID,
                flowName, dependencyName);

        sparkSession.udf().register(MapCountersUDF.NAME, new MapCountersUDF(),
                DataTypes.createMapType(DataTypes.StringType,
                        DataTypes.StringType));
        sparkSession.udf().register(ParseISOTimeUDF.NAME, new ParseISOTimeUDF(),
                DataTypes.TimestampType);

        LOGGER.info("{} files found for parsing", list_of_files.size());

        if (list_of_files.size() > 0) {
            LOGGER.info("Starting PMFileParser");

            String UniqueWorkerName = "PMFileParser";
            String processed_date = getCurrentDate();

            LOGGER.info("Loading reportable entities from MDR");
            HashMap<String, ReportableEntity> persistenceEntities = mdr
                    .getPersistenceEntities(techPackID, "RAW");
            LOGGER.info("Loaded {} reportable entities from MDR",
                    persistenceEntities.size());

            persistenceEntities = mdr.getParitionColumns(persistenceEntities,
                    techPackID, "RAW");

            for (int i = 0; i < Math.ceil(list_of_files.size() / 3000d); i++) {
                final List<String> files_to_parse = list_of_files.subList(
                        i * 3000,
                        Math.min(list_of_files.size(), (i + 1) * 3000));

                try {
                    LOGGER.info("Parsing files");
                    Dataset<Row> sourceData = sparkSession.read()
                            .format("xml-file-v2")
                            .option("rowTag",
                                    configuration.getProperty("rowTag"))
                            .option("rootTag",
                                    configuration.getProperty("rootTag"))
                            .option(DELIMITER_KEY, FLS_PATHS_DELIMITER)
                            .schema(getFileSchema()).load(String
                                    .join(FLS_PATHS_DELIMITER, files_to_parse));

                    final Dataset<Row> pmDataFrame = sourceData.select(
                            col("mdc.mfh.sn").as("nedn"),
                            expr("inline(mdc.md)"));

                    final Dataset<Row> pmDataFrame1 = pmDataFrame
                            .select(col("nedn"), expr("inline(mi)"));

                    final Dataset<Row> pmDataFrame2 = pmDataFrame1.select(
                            callUDF(ParseISOTimeUDF.NAME, col("mts"),
                                    lit(false)).as("ropendtime"),
                            callUDF(ParseISOTimeUDF.NAME, col("mts"), lit(true))
                                    .alias("local_timestamp"),
                            expr("substring(mts, 1, 8)").as("date_id"),
                            col("nedn"), col("gp"), col("mt"),
                            explode(col("mv")).as("mvList"));

                    final Dataset<Row> pmDataFrame3 = pmDataFrame2
                            .withColumn("utc_timestamp", col("ropendtime"));

                    final Dataset<Row> pmDataFrame4 = pmDataFrame3.select(
                            col("ropendtime"), col("utc_timestamp"),
                            col("local_timestamp"), col("date_id"), col("nedn"),
                            col("gp"), col("mvList.moid").as("moid"), col("mt"),
                            col("mvList.r").as("r"));

                    final Dataset<Row> pmDataFrame5 = pmDataFrame4.select(
                            col("ropendtime"), col("utc_timestamp"),
                            col("local_timestamp"), col("date_id"), col("nedn"),
                            col("gp").as("granularity"), col("moid"),
                            callUDF(MapCountersUDF.NAME, col("mt"), col("r"))
                                    .alias("counterMap"));

                    final Dataset<Row> parsedData = pmDataFrame5.select(
                            col("ropendtime"), col("utc_timestamp"),
                            col("local_timestamp"), col("date_id"), col("nedn"),
                            col("granularity"), col("moid"),
                            expr("substring_index(nedn,'=',-1)").as("node"),
                            expr("substring_index(substring_index(moid,',',-1),'=',1)")
                                    .as("measurement"),
                            explode(col("counterMap")).as(new String[] {
                                    "counter_name_raw", "counter_value_raw" }))
                            .drop("counterMap");

                    final Dataset<Row> parsedDataWithoutPDF = parsedData
                            .withColumn("counter_value", expr(
                                    "cast(substring_index(counter_value_raw,',',1)as long)"))
                            .withColumn("counter_name",
                                    expr("lower(counter_name_raw)"))
                            .drop("counter_value_raw", "counter_name_raw");
                    parsedDataWithoutPDF
                            .createOrReplaceTempView(UniqueWorkerName);
                    parsedDataWithoutPDF.cache();

                    LOGGER.info("Parsed data with {} rows",
                            parsedDataWithoutPDF.count());

                    for (final Map.Entry<String, ReportableEntity> entry : persistenceEntities
                            .entrySet()) {
                        final long startTime = System.currentTimeMillis();
                        final String meas = entry.getKey();
                        LOGGER.info("Processing data for {}", meas);
                        ReportableEntity entity = entry.getValue();

                        final String sql = "SELECT "
                                + mergeColumnsQuery(
                                        entity.getColumns().get("key"))
                                + ", measurement, counter_name, counter_value FROM "
                                + UniqueWorkerName + " where measurement = '"
                                + entity.getMOname() + "' and counter_name in ("
                                + mergeColumns(
                                        entity.getColumns().get("counter"))
                                + ")";

                        Dataset<Row> data = sparkSession.sql(sql);

                        ArrayList<String> paritioning = entity.getParitionby();

                        data = data
                                .groupBy("granularity", "moid", "nedn", "node",
                                        "ropendtime", "date_id", "measurement")
                                .pivot("counter_name",
                                        Arrays.asList(entity.getColumns()
                                                .get("counter").toArray()))
                                .agg(first("counter_value"))
                                .drop("measurement", "counter_name");

                        outputProps.put("dbtable", String
                                .format(TABLE_NAME_PATTERN, techPackID, meas));

                        data.write().format("jdbc")
                                .partitionBy(paritioning.stream()
                                        .toArray(String[]::new))
                                .mode(SaveMode.Append).options(outputProps)
                                .save();
                        LOGGER.info("Processed data for {} in {} ms", meas,
                                getElapsedTime(startTime));
                    }

                    parsedDataWithoutPDF.unpersist();

                    updateFileBacklog(files_to_parse,
                            sourceData.select(col("_filename")).distinct(),
                            processed_date);

                } catch (Exception ex) {
                    if (ex instanceof FileNotFoundException) {
                        String errorfilename = ex.getMessage();
                        errorfilename = errorfilename.substring(
                                errorfilename.lastIndexOf("file:"),
                                errorfilename.lastIndexOf(".xml") + 4);
                        errorfilename = errorfilename.replace("file:/",
                                "file:///");

                        mdr.updateFileBacklogError(techPackID, flowName,
                                errorfilename, processed_date);
                    }
                }
            }
        }
        LOGGER.info("PMFileParser completed in {} ms",
                getElapsedTime(parseStartTime));
    }

    public void updateFileBacklog(List<String> files_to_parse,
            Dataset<Row> parsedFilenames, String processed_date) {

        final Set<String> failedFiles = new HashSet<>(files_to_parse);
        final List<String> parsedFiles = parsedFilenames
                .map(r -> (String) r.get(0), Encoders.STRING()).collectAsList();
        
        for(final String file : parsedFiles) {
            failedFiles.remove(file);
        }

        mdr.updateFileBacklog(techPackID, flowName, parsedFiles, processed_date,
                "Parsed");

        mdr.updateFileBacklog(techPackID, flowName,
                failedFiles, processed_date, "Dropped");
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

    public String mergeColumns(Row[] dataRows) {
        String sql = "";
        for (Row row : dataRows) {
            if (!sql.equals("")) {
                sql = sql + ", ";
            }

            sql = sql + row.get(0);
        }
        return sql;
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

    private long getElapsedTime(final long start) {
        return System.currentTimeMillis() - start;
    }

    private String getCurrentDate() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        return dateformat.format(cal.getTime());
    }

    private static StructType getFileSchema() {
        //Schema for mfh top level element
        final StructField ffv = DataTypes.createStructField("ffv",
                DataTypes.StringType, true);
        final StructField sn = DataTypes.createStructField("sn",
                DataTypes.StringType, true);
        final StructField st = DataTypes.createStructField("st",
                DataTypes.StringType, true);
        final StructField vn = DataTypes.createStructField("vn",
                DataTypes.StringType, true);
        final StructField cbt = DataTypes.createStructField("cbt",
                DataTypes.StringType, true);
        final StructField mfh = DataTypes
                .createStructField("mfh",
                        DataTypes.createStructType(
                                new StructField[] { ffv, sn, st, vn, cbt }),
                        true);

        //Schema for mff top level element
        final StructField ts = DataTypes.createStructField("ts",
                DataTypes.StringType, true);
        final StructField mff = DataTypes.createStructField("mff",
                DataTypes.createStructType(new StructField[] { ts }), true);

        //Schema for md top level element        
        final StructField neun = DataTypes.createStructField("neun",
                DataTypes.StringType, true);
        final StructField nedn = DataTypes.createStructField("nedn",
                DataTypes.StringType, true);
        final StructField nesw = DataTypes.createStructField("nesw",
                DataTypes.StringType, true);
        final StructField neid = DataTypes.createStructField("neid", DataTypes
                .createStructType(new StructField[] { neun, nedn, nesw }),
                true);

        final StructField mts = DataTypes.createStructField("mts",
                DataTypes.StringType, true);
        final StructField gp = DataTypes.createStructField("gp",
                DataTypes.StringType, true);
        final StructField mt = DataTypes.createStructField("mt",
                DataTypes.createArrayType(DataTypes.StringType), true);
        final StructField moid = DataTypes.createStructField("moid",
                DataTypes.StringType, true);
        final StructField rP = DataTypes.createStructField("_p",
                DataTypes.LongType, false);
        final StructField rValue = DataTypes.createStructField("_VALUE",
                DataTypes.StringType, false);
        final StructField r = DataTypes.createStructField("r",
                DataTypes.createArrayType(DataTypes
                        .createStructType(new StructField[] { rP, rValue })),
                true);
        final StructField mv = DataTypes.createStructField("mv",
                DataTypes.createArrayType(DataTypes
                        .createStructType(new StructField[] { moid, r }), true),
                true);
        final StructField mi = DataTypes.createStructField("mi",
                DataTypes.createArrayType(DataTypes.createStructType(
                        new StructField[] { mts, gp, mt, mv })),
                true);
        final StructField md = DataTypes.createStructField("md",
                DataTypes.createArrayType(DataTypes
                        .createStructType(new StructField[] { neid, mi })),
                true);

        final StructField mdc = DataTypes.createStructField("mdc",
                DataTypes.createStructType(new StructField[] { mfh, md, mff }),
                false);

        final StructField path = DataTypes.createStructField("_path",
                DataTypes.StringType, false);

        final StructType schema = DataTypes
                .createStructType(new StructField[] { path, mdc });
        return schema;
    }

    private static Map<String, String> getOutputProperties() {
        final Map<String, String> outputProps = new HashMap<>();
        outputProps.put("url", getEnvironmentValue("HSJDBCConnection",
                "jdbc:vertica://ieatrcx7673-2.athtem.eei.ericsson.se:31369/kyo_raw?SearchPath=kyo_vertica_pm_schema"));
        outputProps.put("user",
                getEnvironmentValue("HSWriteUser", "kyo_pm_write"));
        outputProps.put("password",
                getEnvironmentValue("HSWritePassword", "kyopmwrite"));
        outputProps.put("driver",
                getEnvironmentValue("HSDriver", "com.vertica.jdbc.Driver"));
        return outputProps;
    }

}
