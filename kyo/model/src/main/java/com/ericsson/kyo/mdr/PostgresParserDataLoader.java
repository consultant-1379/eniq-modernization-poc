package com.ericsson.kyo.mdr;

import static com.ericsson.oss.services.sonom.common.env.Environment.getEnvironmentValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresParserDataLoader {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(PostgresParserDataLoader.class);
    private Properties mdrJdbcProperties;
    private String mdrJdbcConnection;

    public PostgresParserDataLoader() {
        mdrJdbcConnection = getEnvironmentValue("MDRJDBCConnection",
                "jdbc:postgresql://localhost:5555/MDR");
        mdrJdbcProperties = new Properties();
        mdrJdbcProperties.setProperty("user",
                getEnvironmentValue("MDRUser", "MDR_dba"));
        mdrJdbcProperties.setProperty("password",
                getEnvironmentValue("MDRPassword", "dba123"));
        mdrJdbcProperties.setProperty("driver",
                getEnvironmentValue("MDRDriver", "org.postgresql.Driver"));
    }

    public ArrayList<FlowProperties> getSchedulingRequirements() {
        ArrayList<FlowProperties> schedulings = new ArrayList<FlowProperties>();

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT TECHPACK_VERSION_ID, FLOW_NAME, ACTION_NAME, WORKER_NAME, REOCCURING, ROP_INTERVAL, ROP_OFFSET FROM FLOW_SCHEDULING")) {
            while (resultSet.next()) {
                FlowProperties scheduling = new FlowProperties();
                scheduling.setTechPackID(resultSet.getString(1));
                scheduling.setFlowName(resultSet.getString(2));
                scheduling.setActionName(resultSet.getString(3));
                scheduling.setWorkerName(resultSet.getString(4));
                scheduling
                        .setReoccuring(Boolean.valueOf(resultSet.getString(5)));
                scheduling.setInterval(resultSet.getLong(6));
                scheduling.setOffset(resultSet.getLong(7));
                schedulings.add(scheduling);
            }
        } catch (final SQLException e) {
            LOGGER.error("Error getting schedule information", e);
        }
        return schedulings;

    }

    public Properties getWorkerConfig(String TechPackID, String FlowName,
            String WorkerName) {
        Properties props = new Properties();
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT WORKER_CONFIG FROM FLOW_CONFIGURATION WHERE "
                                + "TECHPACK_VERSION_ID = '" + TechPackID
                                + "' AND " + "FLOW_NAME = '" + FlowName
                                + "' AND " + "WORKER_NAME = '" + WorkerName
                                + "'")) {
            while (resultSet.next()) {
                String temp = resultSet.getString(1);
                String[] properties = temp.split("::");

                for (String property : properties) {
                    String[] propertyparts = property.split("=");
                    props.setProperty(propertyparts[0], propertyparts[1]);
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Error getting worker config", e);
        }
        return props;

    }

    public ArrayList<FlowProperties> getDependentWorker(String TechPackID,
            String FlowName, String ActionName) {
        ArrayList<FlowProperties> properties = new ArrayList<FlowProperties>();

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT TECHPACK_VERSION_ID, FLOW_NAME, WORKER_NAME FROM FLOW_CONFIGURATION WHERE "
                                + "DEPENDENCY_FLOW_NAME = '" + FlowName
                                + "' AND " + "DEPENDENCY_ACTION_NAME = '"
                                + ActionName + "'")) {
            while (resultSet.next()) {
                FlowProperties props = new FlowProperties();
                props.setTechPackID(resultSet.getString(1));
                props.setFlowName(resultSet.getString(2));
                props.setWorkerName(resultSet.getString(3));
                props.setDependencyFlow(FlowName);
                props.setDependencyWorker(ActionName);
                properties.add(props);
            }
        } catch (final SQLException e) {
            LOGGER.error("Error getting dependent worker information", e);
        }
        return properties;

    }

    public void addToFileBacklog(String TechPackID, String FlowName,
            String WorkerName, String filename, String received_date) {

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                PreparedStatement pstmt = connection.prepareStatement(
                        "INSERT INTO FILEBACKLOG(TECHPACK_NAME, FLOW_NAME, WORKER_NAME, FILEPATH, RECEIVED_DATE) VALUES (?, ?, ?, ?, ?)")) {
            pstmt.setString(1, TechPackID);
            pstmt.setString(2, FlowName);
            pstmt.setString(3, WorkerName);
            pstmt.setString(4, filename);
            pstmt.setString(5, received_date);

            pstmt.executeUpdate();
        } catch (final Exception e) {
            LOGGER.error("Error adding to the file backlog", e);
        }

    }

    public void addToFileBacklog(String TechPackID, String FlowName,
            String WorkerName, Collection<String> filenames,
            String received_date) {
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties)) {
            String sql = "INSERT INTO FILEBACKLOG(TECHPACK_NAME, FLOW_NAME, WORKER_NAME, FILEPATH, RECEIVED_DATE) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement pstmt = connection.prepareStatement(sql);

            for (final String filename : filenames) {
                pstmt.setString(1, TechPackID);
                pstmt.setString(2, FlowName);
                pstmt.setString(3, WorkerName);
                pstmt.setString(4, filename);
                pstmt.setString(5, received_date);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

        } catch (final Exception e) {
            LOGGER.error("Error adding to the file backlog", e);
        }
    }

    public void updateFileBacklog(String TechPackID, String FlowName,
            String FilePath, String processed_date, String status) {
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                PreparedStatement pstmt = connection
                        .prepareStatement("UPDATE FILEBACKLOG "
                                + "SET STATUS = '" + status + "', "
                                + "PROCESSED_DATE = '" + processed_date + "' "
                                + "WHERE TECHPACK_NAME = '" + TechPackID + "' AND "
                                + "FLOW_NAME = '" + FlowName + "' AND "
                                + "FILEPATH IN (" + FilePath + ")")) {

            pstmt.executeUpdate();
        } catch (final Exception e) {
            LOGGER.error("Error updating the file backlog", e);
        }
    }
    
    public void updateFileBacklogError(String TechPackID, String FlowName,
            String FilePath, String processed_date) {
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                PreparedStatement pstmt = connection
                        .prepareStatement("UPDATE FILEBACKLOG "
                                + "SET STATUS = 'Failed', "
                                + "PROCESSED_DATE = '" + processed_date + "' " 
                                + "WHERE TECHPACK_NAME = '" + TechPackID + "' AND "
                                + "FLOW_NAME = '" + FlowName + "' AND "
                                + "FILEPATH IN (" + FilePath + ")")) {

            pstmt.executeUpdate();
        } catch (final Exception e) {
            LOGGER.error("Error updating the file backlog", e);
        }
    }

    public void updateFileBacklog(String TechPackID, String FlowName,
            Collection<String> filepaths, String processed_date, String status) {
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties)) {
            String sql = "UPDATE FILEBACKLOG " + "SET STATUS = '" + status + "',"
                    + "PROCESSED_DATE = '" + processed_date + "' "
                    + "WHERE TECHPACK_NAME = '" + TechPackID + "' AND "
                    + "FLOW_NAME = '" + FlowName + "' AND FILEPATH IN ('"
                    + String.join(",", filepaths) + "')";

            PreparedStatement pstmt = connection.prepareStatement(sql);

            pstmt.executeUpdate();
        } catch (final Exception e) {
            LOGGER.error("Error updating the file backlog", e);
        }
    }

    public List<String> getFileBacklog(String TechPackID, String FlowName,
            String WorkerName) {
        List<String> filelist = new ArrayList<String>();

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT DISTINCT FILEPATH FROM FILEBACKLOG WHERE "
                                + "FLOW_NAME = '" + FlowName + "' AND "
                                + "WORKER_NAME = '" + WorkerName
                                + "' AND STATUS IS NULL")) {

            while (resultSet.next()) {
                String filepath = resultSet.getString(1);
                filelist.add(filepath);
            }
        } catch (final Exception e) {
            LOGGER.error("Error getting the file backlog", e);
        }
        return filelist;

    }

    public void deleteOldFileBacklog(String retentionperiod) {

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "DELETE FROM FILEBACKLOG WHERE ropendtime <= CAST((current_date - interval '"
                                + retentionperiod + " days') AS TIMESTAMP);")) {

        } catch (final SQLException e) {
            LOGGER.error("Error cleaning file backlog", e);
        }

    }

    public List<String> getPreviouslyParsedList(String TechPackID,
            String FlowName, String WorkerName) {
        List<String> filelist = new ArrayList<String>();

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT DISTINCT FILEPATH FROM FILEBACKLOG WHERE "
                                + "FLOW_NAME = '" + FlowName + "' AND "
                                + "WORKER_NAME = '" + WorkerName + "' AND STATUS IS NOT NULL")) {
            while (resultSet.next()) {
                String filepath = resultSet.getString(1);
                filelist.add(filepath);
            }
        } catch (final Exception e) {
            LOGGER.error("Error getting the file backlog", e);
        }
        return filelist;
    }

    public HashMap<String, ReportableEntity> getPersistenceEntities(
            String TechPackID, String Entity_Type) {
        HashMap<String, ReportableEntity> persistenceEntities = new HashMap<String, ReportableEntity>();

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT DISTINCT REC.ENTITY_COLUMN_NAME, RE.REPORTABLE_ENTITY_NAME, RE.MAPPED_MO, REC.ENTITY_COLUMN_TYPE, REC.ENTITY_COLUMN_ORDER, RECS.SOURCE_ENTITY_COLUMN_NAME "
                        + "FROM REPORTABLE_ENTITY as RE, REPORTABLE_ENTITY_COLUMNS AS REC, REPORTABLE_ENTITY_COLUMN_SOURCE as RECS "
                                + "WHERE RE.REPORTABLE_ENTITY_ID = REC.REPORTABLE_ENTITY_ID AND "
                                + "REC.ENTITY_COLUMN_ID = RECS.ENTITY_COLUMN_ID AND "
                                + "RE.TECHPACK_VERSION_ID = '" + TechPackID
                                + "' AND " + "RE.ENTITY_TYPE = '" + Entity_Type
                                + "' " + "ORDER BY REC.ENTITY_COLUMN_ORDER")) {
            while (resultSet.next()) {
                String persistenceEntityname = resultSet.getString(2);

                ReportableEntity entity;
                if (persistenceEntities.containsKey(persistenceEntityname)) {
                    entity = persistenceEntities.get(persistenceEntityname);
                } else {
                    entity = new ReportableEntity();
                    entity.setTablename(persistenceEntityname);
                    String[] MOs = (String[]) resultSet.getArray(3).getArray();
                    entity.addMappedMOs(MOs);
                    entity.setMOname(MOs[0]);
                }

                entity.addColumn(resultSet.getString(4),
                        resultSet.getString(1)+":"+resultSet.getString(6));

                persistenceEntities.put(persistenceEntityname, entity);
            }
        } catch (final Exception e) {

            LOGGER.error("Error getting persistence entities", e);
        }
        return persistenceEntities;

    }

    public HashMap<String, ReportableEntity> getParitionColumns(
            HashMap<String, ReportableEntity> persistenceEntities,
            String TechPackID, String Entity_Type) {

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT DISTINCT REC.ENTITY_COLUMN_NAME, RE.REPORTABLE_ENTITY_NAME FROM REPORTABLE_ENTITY AS RE, REPORTABLE_ENTITY_COLUMNS AS REC  "
                                + "WHERE RE.REPORTABLE_ENTITY_ID = REC.REPORTABLE_ENTITY_ID AND "
                                + "RE.TECHPACK_VERSION_ID = '" + TechPackID
                                + "' AND " + "RE.ENTITY_TYPE = '" + Entity_Type
                                + "' AND " + "REC.PARTITION_BY=1")) {
            while (resultSet.next()) {
                String persistenceEntityname = resultSet.getString(2);

                ReportableEntity entity = persistenceEntities
                        .get(persistenceEntityname);
                entity.addParitionby(resultSet.getString(1));

                persistenceEntities.put(persistenceEntityname, entity);

            }
        } catch (final Exception e) {
            LOGGER.error("Error getting partition columns", e);
        }
        return persistenceEntities;

    }

    public HashMap<String, ReportableEntity> getEntities4Partitioning() {
        HashMap<String, ReportableEntity> partitioningData = new HashMap<String, ReportableEntity>();

        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT r_e.reportable_entity_id,t_v.techpack_name, t_v.techpack_release, r_e.reportable_entity_name,  '/'||REPLACE(t_v.techpack_name,'_','/')||'/'||t_v.techpack_release||'/'||r_e.reportable_entity_name, r_e.retention_plan, rp.retention_period"
                                + " FROM reportable_entity r_e, techpack_versions t_v, retention_plan rp WHERE r_e.entity_type LIKE 'RAW' AND r_e.techpack_version_id=t_v.techpack_version_id and r_e.retention_plan=rp.plan_name;")) {

            while (resultSet.next()) {
                String reportable_entity_id = resultSet.getString(1);

                ReportableEntity entity;
                if (partitioningData.containsKey(reportable_entity_id)) {
                    entity = partitioningData.get(reportable_entity_id);
                } else {
                    entity = new ReportableEntity();
                    entity.setTP(resultSet.getString(2));
                    entity.setVersion(resultSet.getString(3));
                    entity.setTablename(resultSet.getString(4));
                    entity.setLoc_Par(resultSet.getString(5));
                    entity.setRetentionPlan(resultSet.getString(6));
                    entity.setRetentionPeriod(resultSet.getInt(7));
                    entity.setMOname(reportable_entity_id);
                }

                partitioningData.put(reportable_entity_id, entity);

            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting reportable entities for partitioning",
                    e);
        }
        return partitioningData;
    }
    
    public HashMap<String, ReportableEntity> getEntities4Partitioning(String TechPackName) {
        HashMap<String, ReportableEntity> partitioningData = new HashMap<String, ReportableEntity>();

        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT r_e.reportable_entity_id,t_v.techpack_name, t_v.techpack_release, r_e.reportable_entity_name,  '/'||REPLACE(t_v.techpack_name,'_','/')||'/'||t_v.techpack_release||'/'||r_e.reportable_entity_name, r_e.retention_plan, rp.retention_period"
                                + " FROM reportable_entity r_e, techpack_versions t_v, retention_plan rp WHERE r_e.entity_type LIKE 'RAW' AND r_e.techpack_version_id=t_v.techpack_version_id and r_e.retention_plan=rp.plan_name and t_v.techpack_name like '"+TechPackName+"';")) {

            while (resultSet.next()) {
                String reportable_entity_id = resultSet.getString(1);

                ReportableEntity entity;
                if (partitioningData.containsKey(reportable_entity_id)) {
                    entity = partitioningData.get(reportable_entity_id);
                } else {
                    entity = new ReportableEntity();
                    entity.setTP(resultSet.getString(2));
                    entity.setVersion(resultSet.getString(3));
                    entity.setTablename(resultSet.getString(4));
                    entity.setLoc_Par(resultSet.getString(5));
                    entity.setRetentionPlan(resultSet.getString(6));
                    entity.setRetentionPeriod(resultSet.getInt(7));
                    entity.setMOname(reportable_entity_id);
                }

                partitioningData.put(reportable_entity_id, entity);

            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting reportable entities for partitioning",
                    e);
        }
        return partitioningData;
    }

    public HashMap<String, ReportableEntity> getEntities4PartitioningPerTechPack(
            String techpack) {
        HashMap<String, ReportableEntity> partitioningData = new HashMap<String, ReportableEntity>();

        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT r_e.reportable_entity_id,t_v.techpack_name, t_v.techpack_release, r_e.reportable_entity_name,  '/'||REPLACE(r_e.techpack_version_id,'_','/')||'/'||r_e.reportable_entity_name, r_e.retention_plan, rp.retention_period"
                                + " FROM reportable_entity r_e, techpack_versions t_v, retention_plan rp WHERE r_e.entity_type LIKE 'RAW' AND r_e.techpack_version_id=t_v.techpack_version_id and r_e.retention_plan=rp.plan_name and t_v.techpack_name like '"
                                + techpack + "';");) {
            while (resultSet.next()) {
                String reportable_entity_id = resultSet.getString(1);

                ReportableEntity entity;
                if (partitioningData.containsKey(reportable_entity_id)) {
                    entity = partitioningData.get(reportable_entity_id);
                } else {
                    entity = new ReportableEntity();
                    entity.setTP(resultSet.getString(2));
                    entity.setVersion(resultSet.getString(3));
                    entity.setTablename(resultSet.getString(4));
                    entity.setLoc_Par(resultSet.getString(5));
                    entity.setRetentionPlan(resultSet.getString(6));
                    entity.setRetentionPeriod(resultSet.getInt(7));
                    entity.setMOname(reportable_entity_id);
                }

                partitioningData.put(reportable_entity_id, entity);

            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting reportable entities for partitioning",
                    e);
        }
        return partitioningData;
    }

    public HashMap<String, ReportableEntity> getEntities4PartitioningPerTechPack(
            String techpack, String TechPackVersion) {
        HashMap<String, ReportableEntity> partitioningData = new HashMap<String, ReportableEntity>();
        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT r_e.reportable_entity_id,t_v.techpack_name, t_v.techpack_release, r_e.reportable_entity_name,  '/'||REPLACE(r_e.techpack_version_id,'_','/')||'/'||r_e.reportable_entity_name, r_e.retention_plan, rp.retention_period"
                                + " FROM reportable_entity r_e, techpack_versions t_v, retention_plan rp WHERE r_e.entity_type LIKE 'RAW' AND r_e.techpack_version_id=t_v.techpack_version_id and r_e.retention_plan=rp.plan_name and t_v.techpack_name like '"
                                + techpack
                                + "' and t_v.techpack_version_id like '"
                                + TechPackVersion + "';");) {

            while (resultSet.next()) {
                String reportable_entity_id = resultSet.getString(1);

                ReportableEntity entity;
                if (partitioningData.containsKey(reportable_entity_id)) {
                    entity = partitioningData.get(reportable_entity_id);
                } else {
                    entity = new ReportableEntity();
                    entity.setTP(resultSet.getString(2));
                    entity.setVersion(resultSet.getString(3));
                    entity.setTablename(resultSet.getString(4));
                    entity.setLoc_Par(resultSet.getString(5));
                    entity.setRetentionPlan(resultSet.getString(6));
                    entity.setRetentionPeriod(resultSet.getInt(7));
                    entity.setMOname(reportable_entity_id);
                }
                partitioningData.put(reportable_entity_id, entity);
            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting reportable entities for partitioning",
                    e);
        }
        return partitioningData;
    }

    public HashMap<String, ReportableEntity> getREcolumnDetails(String ReID,
            String techpack_version_id) {
        HashMap<String, ReportableEntity> REcolumnDetails = new HashMap<String, ReportableEntity>();
        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT reportable_entity_id,entity_column_name,entity_data_type,partition_by,techpack_version_id FROM reportable_entity_columns  WHERE  reportable_entity_id like '"
                                + ReID + "' and techpack_version_id like '"
                                + techpack_version_id
                                + "' order by entity_column_order;")) {
            ReportableEntity entity;
            entity = new ReportableEntity();
            while (resultSet.next()) {
                String reportable_entity_id = resultSet.getString(1);
                entity.addColumn(resultSet.getString(2));
                entity.addColumnDataTypes(resultSet.getString(3));
                entity.addParitionbyFlag(resultSet.getInt(4));
                entity.setVersion(resultSet.getString(5));
                REcolumnDetails.put(reportable_entity_id, entity);
            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting partition columns ", e);
        }
        return REcolumnDetails;
    }

    public List<String> getReportableEntityName_IDPerTechPack(
            String techpack_version_id) {
        List<String> Relist = new ArrayList<String>();
        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT distinct  reportable_entity_name,reportable_entity_id FROM reportable_entity where techpack_version_id like '"
                                + techpack_version_id
                                + "' and entity_type like 'RAW';");) {

            while (resultSet.next()) {
                String re_ID = resultSet.getString(1) + ","
                        + resultSet.getString(2);
                Relist.add(re_ID);
            }
        } catch (final Exception e) {
            e.printStackTrace();
            LOGGER.warn(
                    "Error getting the reportable entity name & ID for TechPack version {}",
                    techpack_version_id, e);
        }
        return Relist;

    }

    public List<String> getReportableEntityNamePerTechPack(String techpack) {
        List<String> Relist = new ArrayList<String>();

        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT distinct  reportable_entity_name FROM reportable_entity where techpack_version_id in "
                                + "( select techpack_version_id from techpack_versions where techpack_name like '"
                                + techpack + "')"
                                + " and entity_type like 'RAW';")) {

            while (resultSet.next()) {
                String re = resultSet.getString(1);
                Relist.add(re);
            }
        } catch (final Exception e) {
            LOGGER.warn(
                    "Error getting the distinct reportable entity name for TechPack {}",
                    techpack, e);
        }
        return Relist;

    }

    public String getTechPackDetails(String techpack_version_id) {
        String TechPackDetails = new String();
        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT techpack_name,techpack_release FROM techpack_versions where techpack_version_id like '"
                                + techpack_version_id + "';");) {

            while (resultSet.next()) {
                TechPackDetails = resultSet.getString(1) + ","
                        + resultSet.getString(2);

            }
        } catch (final Exception e) {
            LOGGER.warn(
                    "Error getting the techpack details  for TechPack version {}",
                    techpack_version_id, e);
        }
        return TechPackDetails;

    }

    public void updateReportableEntity(String techpack_name,
            String reportable_entity_name, String reportable_entity_description,
            String entity_type, String partition_plan, String retention_plan,
            String mapped_mo) {
        String sql = "DELETE FROM reportable_entity_columns WHERE reportable_entity_id='"
                + techpack_name + ":" + reportable_entity_name + "';"
                + "DELETE FROM reportable_entity WHERE "
                + " techpack_version_id='" + techpack_name + "' and "
                + " reportable_entity_name='" + reportable_entity_name
                + "' and " + " reportable_entity_id='" + techpack_name + ":"
                + reportable_entity_name + "' and " + " entity_type like '"
                + entity_type + "';" + "INSERT INTO reportable_entity "
                + "    ( techpack_version_id, reportable_entity_name, description,reportable_entity_id, entity_type, partition_plan,retention_plan,mapped_mo) "
                + "    VALUES " + "    ( " + "        '" + techpack_name + "', "
                + "        '" + reportable_entity_name + "', " + "        '"
                + reportable_entity_description + "', " + "        '"
                + techpack_name + ":" + reportable_entity_name + "', "
                + "        '" + entity_type + "', " + "        '"
                + partition_plan + "', " + "        '" + retention_plan + "', "
                + "        '" + mapped_mo + "' );";
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (final Exception e) {
            LOGGER.error("Error updating the the reportable entity for {}",
                    reportable_entity_name, e);
        }
    }

    public void updateReportableEntityColumns(String techpack_name,
            String reportable_entity_name, String column_name,
            String column_type, String entity_column_description) {
        String sql = "DELETE FROM reportable_entity_columns WHERE "
                + " techpack_version_id='" + techpack_name + "' and "
                + " reportable_entity_id='" + techpack_name + ":"
                + reportable_entity_name + "' and " + " entity_column_name='"
                + column_name + "' and " + " entity_data_type='" + column_type
                + "' and " + " entity_column_id='" + techpack_name + ":"
                + reportable_entity_name + ":" + column_name + "';"
                + "INSERT INTO reportable_entity_columns "
                + "    ( techpack_version_id, reportable_entity_id, entity_column_name, entity_data_type,entity_column_id, entity_column_description ) "
                + "    VALUES " + "    ( " + "        '" + techpack_name + "', "
                + "        '" + techpack_name + ":" + reportable_entity_name
                + "', " + "        '" + column_name + "', " + "        '"
                + column_type + "', " + "        '" + techpack_name + ":"
                + reportable_entity_name + ":" + column_name + "', "
                + "        '" + entity_column_description + "');";
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (final Exception e) {
            LOGGER.error("Error updating the the reportable entity for {}",
                    reportable_entity_name, e);
        }
    }

    public HashMap<String, HashMap<String, String>> getDetails2CreateRE(
            String Version, String TP) {
        String searchParameter = TP.replace(Version, "%");
        String Sql = "SELECT " + "    pe.persistence_entity_name, "
                + "    pe.persistence_entity_name||'_'||pe.node_model_version_id, "
                + "    pec.column_name||':X:'||pec.data_type||':X:'||pec.counter_type "
                + "    FROM" + "    persistence_entity         AS pe, "
                + "    persistence_entity_columns AS pec " + "    WHERE"
                + "    pe.techpack_name=pec.techpack_name "
                + "    AND pe.node_model_version_id=pec.node_model_version_id "
                + "    AND pe.techpack_name LIKE '" + searchParameter + "' "
                + "    AND pe.persistence_entity_name=pec.persistence_entity_name "
                + "    GROUP BY " + "    pe.persistence_entity_name,"
                + "    pe.persistence_entity_name||'_'||pe.node_model_version_id,"
                + "    pec.column_name||':X:'||pec.data_type||':X:'||pec.counter_type "
                + "    ORDER BY "
                + "    persistence_entity_name,pec.column_name||':X:'||pec.data_type||':X:'||pec.counter_type;";

        HashMap<String, HashMap<String, String>> REcolumnDetails = new HashMap<String, HashMap<String, String>>();
        String REname = null;
        String PEname = null;
        String ColumnDetails = null;
        HashMap<String, String> colHashMap = new HashMap<String, String>();
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(Sql)) {

            while (resultSet.next()) {

                REname = resultSet.getString(1);
                PEname = resultSet.getString(3);

                if (REcolumnDetails.containsKey(REname)) {
                    colHashMap = REcolumnDetails.get(REname);

                    if (colHashMap.containsKey(PEname)) {

                        ColumnDetails = colHashMap.get(PEname) + "::"
                                + resultSet.getString(2);
                        colHashMap.put(resultSet.getString(3), ColumnDetails);

                    } else {
                        colHashMap.put(resultSet.getString(3),
                                resultSet.getString(2));
                    }

                    REcolumnDetails.put(REname, colHashMap);
                } else {
                    colHashMap = new HashMap<String, String>();
                    colHashMap.put(resultSet.getString(3),
                            resultSet.getString(3));
                    REcolumnDetails.put(REname, colHashMap);
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Error getting partition columns ", e);
        }
        return REcolumnDetails;
    }

    public String getVersionInfo4TechPack(String techpack_version_id) {
        String version = new String();
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT  techpack_name||':'||techpack_release FROM techpack_versions where techpack_version_id like '"
                                + techpack_version_id + "';")) {

            while (resultSet.next()) {
                version = resultSet.getString(1);
            }
        } catch (final Exception e) {
            LOGGER.warn(
                    "Error getting the persistence entity name for TechPack {}",
                    techpack_version_id, e);
        }
        return version;
    }

    public ArrayList<String> getVersions4TechPack(String TechPack) {
        ArrayList<String> versions = new ArrayList<String>();
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "SELECT  techpack_release FROM techpack_versions where techpack_name like '"
                                + TechPack + "';")) {
            while (resultSet.next()) {
                versions.add(resultSet.getString(1));
            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting the versions for TechPack {}", TechPack,
                    e);
        }
        return versions;

    }

    public List<String> getColListPerPE(String REID, String TP) {
        List<String> colList = new ArrayList<String>();

        String SQL = "SELECT DISTINCT " + "    entity_column_name " + " FROM "
                + "    reportable_entity_columns " + " WHERE "
                + "    reportable_entity_id like  '" + REID + "';";
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(SQL)) {

            while (resultSet.next()) {
                String re = resultSet.getString(1);
                colList.add(re);
            }
        } catch (final Exception e) {
            LOGGER.warn(
                    "Error getting the distinct column list for RE {} for TechPack {}",
                    REID, TP, e);
        }
        return colList;

    }

    public String getColDataTypePerColVersion(String RE, String TP,
            String Version, String colName) {
        String dataType = new String();

        final String sql = "SELECT  " + "    entity_data_type " + " FROM "
                + "    reportable_entity_columns " + " WHERE "
                + "    reportable_entity_id IN " + "    ( " + "        SELECT "
                + "            reportable_entity_id " + "        FROM "
                + "            reportable_entity " + "        WHERE"
                + "            techpack_version_id like " + "            ( "
                + "                SELECT "
                + "                    techpack_version_id "
                + "                FROM "
                + "                    techpack_versions "
                + "                WHERE "
                + "                    techpack_name LIKE '" + TP + "' "
                + "                    and techpack_release like '" + Version
                + "') " + "        AND entity_type LIKE 'RAW' "
                + "        AND reportable_entity_name LIKE '" + RE + "') "
                + "        and entity_column_name like '" + colName + "';";
        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                dataType = resultSet.getString(1);

            }
            if (dataType.isEmpty()) {
                dataType = "NotInVersion";
            }
        } catch (final Exception e) {
            LOGGER.warn(
                    "Error getting the column data type per version for {} for version {}",
                    RE, Version, e);
        }
        return dataType;

    }

    public HashMap<Integer, ArrayList<String>> getHSRetention(
            String techpack_version_id, String Entity_Type) {
        HashMap<Integer, ArrayList<String>> retentionperiods = new HashMap<Integer, ArrayList<String>>();

        try (final Connection connection = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "select re.reportable_entity_name, rp.hot_store_retention_period from reportable_entity as re, retention_plan as rp "
                                + "where re.retention_plan = rp.plan_name "
                                + "and re.entity_type = '" + Entity_Type + "'"
                                + "and re.techpack_version_id = '"
                                + techpack_version_id + "';")) {
            while (resultSet.next()) {
                Integer retentionperiod = resultSet.getInt(1);
                String tablename = resultSet.getString(2);

                ArrayList<String> tablenames;
                if (retentionperiods.containsKey(retentionperiod)) {
                    tablenames = retentionperiods.get(retentionperiod);
                } else {
                    tablenames = new ArrayList<String>();
                }

                tablenames.add(tablename);
                retentionperiods.put(retentionperiod, tablenames);

            }
        } catch (final SQLException e) {
            LOGGER.error("Error getting schedule information", e);
        }
        return retentionperiods;

    }
    public String getRetention_Partition_plan(String RE, String reID) {
        String Retention_Partition_plan = new String();
        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "select retention_plan,partition_plan from reportable_entity where reportable_entity_name like '"+RE+"' and reportable_entity_id like '"+reID+"';")) {

            while (resultSet.next()) {
            	Retention_Partition_plan = resultSet.getString(1) + ","
                        + resultSet.getString(2);

            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting the Retention & Partition_plan  for "+RE+" and "+reID, e);
        }
        return Retention_Partition_plan;

    }

	public int getNumTablesSplit(String RE_match, String techpack_version_id) {
		int NumTablesSplit = 0;
        try (final Connection conn = DriverManager
                .getConnection(mdrJdbcConnection, mdrJdbcProperties);
                Statement statement = conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(
                        "select count(*) from reportable_entity where reportable_entity_name like '"+RE_match+"_' and reportable_entity_name ~ '\\d+$' and entity_type like 'RAW' and techpack_version_id like '"+techpack_version_id+"';")) {

            while (resultSet.next()) {
            	NumTablesSplit = resultSet.getInt(1);

            }
        } catch (final Exception e) {
            LOGGER.warn("Error getting the number of time  "+RE_match+" was split.", e);
        }
        return NumTablesSplit;
	}
}