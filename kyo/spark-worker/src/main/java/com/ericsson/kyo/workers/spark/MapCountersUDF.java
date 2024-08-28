/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2019
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.kyo.workers.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;

import scala.collection.*;

public class MapCountersUDF
        implements UDF2<Seq<String>, Seq<Row>, Map<String, String>> {

    private static final long serialVersionUID = -3760112095112127385L;

    public static String NAME = "mapCounters";

    @Override
    public Map<String, String> call(Seq<String> arg0, Seq<Row> arg1)
            throws Exception {
        final java.util.Map<String, String> output = new java.util.HashMap<>();
        for (int i = 0; i < arg1.size(); i++) {
            final Row input = arg1.apply(i);
            final int index = ((Long)input.getAs("_p")).intValue();
            final String value = input.getAs("_VALUE");
            output.put(arg0.apply(index), value);
        }
        return JavaConversions.mapAsScalaMap(output);
    }

}
