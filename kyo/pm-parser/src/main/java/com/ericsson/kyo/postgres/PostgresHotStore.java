package com.ericsson.kyo.postgres;

import static com.ericsson.oss.services.sonom.common.env.Environment.getEnvironmentValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.PostgresParserDataLoader;

public class PostgresHotStore {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(PostgresParserDataLoader.class);
    private Properties hsJdbcPropertiesWrite;
    private String hsJdbcConnectionWrite;
    private Properties hsJdbcPropertiesRead;
    private String hsJdbcConnectionRead;

    
    public PostgresHotStore() {
        hsJdbcConnectionWrite = getEnvironmentValue("HSJDBCConnection",
                "jdbc:postgresql://localhost:5432/kyo_hs");
        hsJdbcPropertiesWrite = new Properties();
        hsJdbcPropertiesWrite.setProperty("user",
                getEnvironmentValue("HSWriteUser", "hs_write"));
        hsJdbcPropertiesWrite.setProperty("password",
                getEnvironmentValue("HSWritePassword", "hs_write"));
        hsJdbcPropertiesWrite.setProperty("driver",
                getEnvironmentValue("HSDriver", "org.postgresql.Driver"));
        
    	hsJdbcConnectionRead = getEnvironmentValue("HSJDBCConnection",
                 "jdbc:postgresql://localhost:5432/kyo_hs");
    	 hsJdbcPropertiesRead = new Properties();
    	 hsJdbcPropertiesRead.setProperty("user",
                 getEnvironmentValue("HSReadUser", "hs_read"));
    	 hsJdbcPropertiesRead.setProperty("password",
                 getEnvironmentValue("HSReadPassword", "hs_read"));
    	 hsJdbcPropertiesRead.setProperty("driver",
                 getEnvironmentValue("HSDriver", "org.postgresql.Driver"));
     }
    
    public Properties getConnectionProperties(String user){
    	if(user.equalsIgnoreCase("READ")) {
    		return hsJdbcPropertiesRead;
    	}else if(user.equalsIgnoreCase("WRITE")) {
    		return hsJdbcPropertiesWrite;
    	}
    	
    	return null;
    }
    
    public String getConnectionURL(String user){
    	if(user.equalsIgnoreCase("READ")) {
    		return hsJdbcConnectionRead;
    	}else if(user.equalsIgnoreCase("WRITE")) {
    		return hsJdbcConnectionWrite;
    	}
    	
    	return null;
    }
    
    public int executeWriteSQLResult(String sql) {
    	int resultSet = 1 ;
        try {
            final Connection connection = DriverManager
                    .getConnection(hsJdbcConnectionWrite, hsJdbcPropertiesWrite);
            Statement statement = connection.createStatement();
            resultSet = statement.executeUpdate(sql);
            statement.close();
            connection.close();
        } catch (final SQLException e) {
        	LOGGER.warn("Error running SQL to MDR", e);
            System.err.println("Error running SQL to MDR" + e);
        }
        return resultSet;
    }
    

        
    public ResultSet executeReadSQLResult(String sql) {
        ResultSet resultSet = null;
        try {
            final Connection connection = DriverManager
                    .getConnection(hsJdbcConnectionRead, hsJdbcPropertiesRead);
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

        } catch (final SQLException e) {
        	LOGGER.warn("Error running SQL to HS", e);
            System.err.println("Error running SQL to HS" + e);
        }
        return resultSet;

    }
    

  }