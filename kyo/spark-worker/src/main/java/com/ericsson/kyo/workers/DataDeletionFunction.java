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
package com.ericsson.kyo.workers;

import java.sql.*;
import java.util.*;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataDeletionFunction implements ForeachPartitionFunction<Row> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(DataDeletionFunction.class);

    private String url;
    private String user;
    private String password;
    private String table;
    
    public DataDeletionFunction(final String url, final Properties properties, final String table) {
        this.url = url;
        this.user = properties.getProperty("user");
        this.password = properties.getProperty("password");
        this.table = table;
    }

    @Override
    public void call(final Iterator<Row> arg0) {
        final List<String> rops = new ArrayList<>();
        while (arg0.hasNext()) {
            rops.add(arg0.next().getString(0));
        }
        if (rops.size() > 0) {
            final String sql = "delete from " + table
                    + " where date_id in ('" + String.join("','", rops)
                    + "')";
            try (final Connection conn = DriverManager.getConnection(url, user,
                    password); final Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            } catch (final Exception e) {
                LOGGER.error("Error deleting rops {}", rops, e);
            }
        }
    }

}
