/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2012
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.kyo.sources;

import static com.databricks.spark.xml.XmlInputFormat.ENCODING_KEY;
import static com.databricks.spark.xml.XmlInputFormat.END_TAG_KEY;
import static com.databricks.spark.xml.XmlInputFormat.START_TAG_KEY;

import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.*;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;

import com.databricks.spark.xml.XmlOptions;
import com.ericsson.oss.services.sonom.pm.stats.parsing.PmStatsParserConstants;
import com.ericsson.oss.services.sonom.pm.stats.parsing.sources.XmlInputFormat;
import com.ericsson.oss.services.sonom.pm.stats.parsing.sources.XmlRelation;

import scala.collection.JavaConversions;
import scala.collection.immutable.Map;

public class XmlFileSource extends
        com.ericsson.oss.services.sonom.pm.stats.parsing.sources.XmlFileSource {

    @Override
    public String shortName() {
        return "xml-file-v2";
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext,
            final Map<String, String> parameters, final StructType schema) {
        final JavaSparkContext context = JavaSparkContext
                .fromSparkContext(sqlContext.sparkContext());
        final java.util.Map<String, String> params = JavaConversions
                .mapAsJavaMap(parameters);
        final Configuration config = context.hadoopConfiguration();
        final String path = params.get("path");
        final String rowTag = params.getOrDefault("rowTag",
                XmlOptions.DEFAULT_ROW_TAG());
        final String delimiter = params
                .get(PmStatsParserConstants.DELIMITER_KEY);
        final XmlOptions options = XmlOptions.apply(parameters);
        final String charset = options.charset();
        config.set(XmlInputFormat.PATHS, path);
        config.set(PmStatsParserConstants.DELIMITER_KEY, delimiter);
        config.set(START_TAG_KEY(), String.format("<%1$s>", rowTag));
        config.set(END_TAG_KEY(), String.format("</%1$s>", rowTag));
        config.set(ENCODING_KEY(), charset);
        final NewHadoopRDD<LongWritable, Text> rdd = new NewHadoopRDD<LongWritable, Text>(
                sqlContext.sparkContext(), XmlInputFormat.class,
                LongWritable.class, Text.class, config);
        final JavaPairRDD<LongWritable, Text> javaRdd = new JavaNewHadoopRDD<>(
                rdd, null, null);
        final RDD<String> lines = javaRdd.map(k -> new String(k._2.getBytes(),
                0, k._2.getLength(), Charset.forName(charset))).rdd();
        return new XmlRelation(sqlContext, lines, schema, options);
    }
}
