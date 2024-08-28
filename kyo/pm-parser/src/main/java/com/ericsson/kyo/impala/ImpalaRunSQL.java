package com.ericsson.kyo.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaRunSQL {

    private static final Logger log = LoggerFactory
            .getLogger(ImpalaRunSQL.class);
    private Properties impalaJdbcProperties;
    private String impalaJdbcConnection;

    public ImpalaRunSQL() {
        impalaJdbcConnection = System.getProperty("IMPALAJDBCConnection",
                "jdbc:impala://localhost:21050;AuthMech=0;SocketTimeout=0");
        impalaJdbcProperties = new Properties();
        impalaJdbcProperties.setProperty("user",
                System.getProperty("IMPALAUser", ""));
        impalaJdbcProperties.setProperty("password",
                System.getProperty("IMPALAPassword", ""));
        impalaJdbcProperties.setProperty("driver", System.getProperty(
                "IMPALADriver", "com.cloudera.impala.jdbc4.Driver"));
    }

    public void executeSQL(String sql) {

        try {
            final Connection connection = DriverManager
                    .getConnection(impalaJdbcConnection, impalaJdbcProperties);
            Statement statement = connection.createStatement();
            statement.execute(sql);
            statement.close();
            connection.close();
        } catch (final SQLException e) {
            log.warn("Error running SQL to Impala", e);
            System.err.println("Error running SQL to Impala" + e);
            e.printStackTrace();
        }

    }

    public ResultSet executeSQLResult(String sql) {
        ResultSet resultSet = null;
        try {
            final Connection connection = DriverManager
                    .getConnection(impalaJdbcConnection, impalaJdbcProperties);
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

        } catch (final SQLException e) {
            log.warn("Error running SQL to Impala", e);
            System.err.println("Error running SQL to Impala" + e);
            e.printStackTrace();
        }
        return resultSet;

    }

    public ArrayList<String> getPartitionListImpala(String sql)
            throws SQLException {
        ResultSet resultSet = null;
        Statement statement = null;
        Connection connection = null;
        ArrayList<String> locationlist = new ArrayList<String>();

        try {
            connection = DriverManager.getConnection(impalaJdbcConnection,
                    impalaJdbcProperties);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                locationlist.add(resultSet.getString("Location"));
            }
        } catch (final SQLException e) {
            log.warn("Error running SQL to Impala", e);
            System.err.println("Error running SQL to Impala" + e);
            e.printStackTrace();
        } finally {
            statement.close();
            connection.close();
        }

        return locationlist;

    }

}