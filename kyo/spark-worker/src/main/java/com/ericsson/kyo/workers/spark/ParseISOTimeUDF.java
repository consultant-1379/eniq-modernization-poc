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
package com.ericsson.kyo.workers.spark;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.spark.sql.api.java.UDF2;

public class ParseISOTimeUDF
        implements UDF2<String, Boolean, Timestamp> {

    private static DateTimeFormatter formatter;

    static {
        formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssX");
    }

    public static String NAME = "isoDateTime";
    
    @Override
    public Timestamp call(final String t1, final Boolean t2) throws Exception {
        if (!t2) {
            return new Timestamp(
                    ZonedDateTime.parse(t1, formatter).toEpochSecond() * 1000);
        }
        return new Timestamp(ZonedDateTime.parse(t1, formatter)
                .toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1000);
    }

}
