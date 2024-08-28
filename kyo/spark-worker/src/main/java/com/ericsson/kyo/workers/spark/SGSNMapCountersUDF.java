package com.ericsson.kyo.workers.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;

import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.Seq;

public class SGSNMapCountersUDF implements UDF2<Seq<Row>, Seq<Row>, Map<String, String>> {

    private static final long serialVersionUID = -3760112095112127385L;

    public static String NAME = "SGSNmapCounters";

    @Override
    public Map<String, String> call(Seq<Row> arg0, Seq<Row> arg1)
            throws Exception {
        final java.util.Map<String, String> output = new java.util.HashMap<>();
        
        final java.util.Map<Integer, String> tmpCtr = new java.util.HashMap<>();
        for (int i = 0; i < arg0.size(); i++) {
        	final Row input = arg0.apply(i);
        	final int index = ((Long)input.getAs("_p")).intValue();
        	final String value = input.getAs("_VALUE");
        	tmpCtr.put(index, value);
        }
        
        for (int i = 0; i < arg1.size(); i++) {
            final Row input = arg1.apply(i);
            final int index = ((Long)input.getAs("_p")).intValue();
            final String value = String.valueOf((Long)input.getAs("_VALUE"));
            output.put(tmpCtr.get(index), value);
        }
        return JavaConversions.mapAsScalaMap(output);
    }

}
