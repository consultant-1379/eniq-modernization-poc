package com.ericsson.kyo.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.mdr.ReportableEntity;
import com.ericsson.kyo.mdr.workers.WorkerBase;
import com.ericsson.kyo.impala.ImpalaRunSQL;

public class AddPartitions extends WorkerBase {

    private static final Logger log = LoggerFactory
            .getLogger(AddPartitions.class);
    private long start = System.currentTimeMillis();
    ImpalaRunSQL impalaExec = new ImpalaRunSQL();

    public void execute() {
        log.info("Executing KYO Partitioning");
        LocalDate givendate = LocalDate.now();
        HashMap<String, ReportableEntity> map4Partitioning = mdr
                .getEntities4Partitioning();

        if (map4Partitioning.size() > 0) {

            for (Map.Entry<String, ReportableEntity> entry : map4Partitioning
                    .entrySet()) {
                ReportableEntity entity = entry.getValue();
                String RE = entity.getTablename();
                Pattern p = Pattern.compile("_\\d+$");
        		Matcher m = p.matcher(RE);
        		if(m.find()) {
            		RE=RE.replaceAll("_\\d+$", "");
        		}
                String TP = entity.getTP();
                String loc_par = entity.getLoc_Par();
                String version = entity.getVersion();
                String RE_ID = entity.getMOname();

                addPartition(RE, TP, loc_par, version, RE_ID, givendate);

            }

            log.info("Partitioning successfully completed in "
                    + getElapsedTime() / 1000 + " seconds.");

        }

    }

    public void execute(LocalDate givendate, String TechPackName) {
        log.info("Executing KYO Partitioning");

        HashMap<String, ReportableEntity> map4Partitioning = mdr
                .getEntities4PartitioningPerTechPack(TechPackName);

        if (map4Partitioning.size() > 0) {

            for (Map.Entry<String, ReportableEntity> entry : map4Partitioning
                    .entrySet()) {
                ReportableEntity entity = entry.getValue();
                String RE = entity.getTablename();
                Pattern p = Pattern.compile("_\\d+$");
        		Matcher m = p.matcher(RE);
        		if(m.find()) {
            		RE=RE.replaceAll("_\\d+$", "");
        		}
                String TP = entity.getTP();
                String loc_par = entity.getLoc_Par();
                String version = entity.getVersion();
                String RE_ID = entity.getMOname();

                addPartition(RE, TP, loc_par, version, RE_ID, givendate);

            }

            log.info("Partitioning successfully completed in "
                    + getElapsedTime() / 1000 + " seconds.");

        }

    }
    public void execute(String TechPackName) {
        log.info("Executing KYO Partitioning");
        LocalDate givendate = LocalDate.now();
        HashMap<String, ReportableEntity> map4Partitioning = mdr
                .getEntities4PartitioningPerTechPack(TechPackName);

        if (map4Partitioning.size() > 0) {

            for (Map.Entry<String, ReportableEntity> entry : map4Partitioning
                    .entrySet()) {
                ReportableEntity entity = entry.getValue();
                String RE = entity.getTablename();
                Pattern p = Pattern.compile("_\\d+$");
        		Matcher m = p.matcher(RE);
        		if(m.find()) {
            		RE=RE.replaceAll("_\\d+$", "");
        		}
                String TP = entity.getTP();
                String loc_par = entity.getLoc_Par();
                String version = entity.getVersion();
                String RE_ID = entity.getMOname();

                addPartition(RE, TP, loc_par, version, RE_ID, givendate);

            }

            log.info("Partitioning successfully completed in "
                    + getElapsedTime() / 1000 + " seconds.");

        }

    }
    
    public void execute(LocalDate givendate, String TechPackName,
            String TechPackVersion) {
        log.info("Executing KYO Partitioning");

        HashMap<String, ReportableEntity> map4Partitioning = mdr
                .getEntities4PartitioningPerTechPack(TechPackName,
                        TechPackVersion);

        if (map4Partitioning.size() > 0) {

            for (Map.Entry<String, ReportableEntity> entry : map4Partitioning
                    .entrySet()) {
                ReportableEntity entity = entry.getValue();
                String RE = entity.getTablename();
                Pattern p = Pattern.compile("_\\d+$");
        		Matcher m = p.matcher(RE);
        		if(m.find()) {
            		RE=RE.replaceAll("_\\d+$", "");
        		}
                String TP = entity.getTP();
                String loc_par = entity.getLoc_Par();
                String version = entity.getVersion();
                String RE_ID = entity.getMOname();

                addPartition(RE, TP, loc_par, version, RE_ID, givendate);

            }

            log.info("Partitioning successfully completed in "
                    + getElapsedTime() / 1000 + " seconds.");

        }

    }

    public void execute(LocalDate givendate) {
        log.info("Executing KYO Partitioning");

        HashMap<String, ReportableEntity> map4Partitioning = mdr
                .getEntities4Partitioning();

        if (map4Partitioning.size() > 0) {

            for (Map.Entry<String, ReportableEntity> entry : map4Partitioning
                    .entrySet()) {
                ReportableEntity entity = entry.getValue();
                String RE = entity.getTablename();
                Pattern p = Pattern.compile("_\\d+$");
        		Matcher m = p.matcher(RE);
        		if(m.find()) {
            		RE=RE.replaceAll("_\\d+$", "");
        		}
                String TP = entity.getTP();
                String loc_par = entity.getLoc_Par();
                String version = entity.getVersion();
                String RE_ID = entity.getMOname();

                addPartition(RE, TP, loc_par, version, RE_ID, givendate);

            }

            log.info("Partitioning successfully completed in "
                    + getElapsedTime() / 1000 + " seconds.");

        }

    }

    public void addPartition(String re, String tp, String loc_par,
            String version, String RE_ID, LocalDate givendate) {

        LocalDate dtOrg = givendate;
        LocalDate dtPlusOne = dtOrg.plusDays(1);
        String dt1 = dtPlusOne.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        String localTablename = re + "_" + version;
        localTablename = localTablename.replace(".", "_");

        String[] TempArray = RE_ID.split(":");
        String techpack_version_id = TempArray[0].trim();

        try {

            HashMap<String, ReportableEntity> map2createRE = mdr
                    .getREcolumnDetails(RE_ID, techpack_version_id);
            ArrayList<String> addPartitionArray = new ArrayList<String>();
            for (Entry<String, ReportableEntity> entry : map2createRE
                    .entrySet()) {
                ReportableEntity entity = entry.getValue();
                ArrayList<String> Columns = entity.getaltColumns();
                ArrayList<Integer> ParitionbyFlags = entity
                        .getParitionbyFlags();

                ArrayList<String> cols2Partition = new ArrayList<String>();

                for (int i = 0; i < Columns.size(); i++) {
                    if (ParitionbyFlags.get(i) == 1) {
                        cols2Partition.add(Columns.get(i));
                    }
                }

                if (cols2Partition.size() == 1) {
                    String partitioningString = new String();
                    String locationPartitioningString = new String();
                    if (cols2Partition.get(0).equals("date_id")) {
                        partitioningString = cols2Partition.get(0) + "='" + dt1
                                + "'";
                        locationPartitioningString = cols2Partition.get(0) + "="
                                + dt1 + "";
                    }

                    String addPartionPE = "ALTER TABLE " + localTablename
                            + " ADD IF NOT EXISTS PARTITION ("
                            + partitioningString + ") LOCATION '" + loc_par
                            + "/" + locationPartitioningString + "';";
                    addPartitionArray.add(addPartionPE);

                } else if (cols2Partition.size() == 2) {

                    String partitioningString = new String();
                    String locationPartitioningString = new String();
                    if (cols2Partition.get(1).equals("hour_id")) {

                        for (int i = 0; i < 24; i++) {
                            String hr = "";

                            if (i < 10) {
                                hr = "0" + i;

                            } else {
                                hr = Integer.toString(i);
                            }
                            partitioningString = cols2Partition.get(0) + "='"
                                    + dt1 + "'," + cols2Partition.get(1) + "='"
                                    + hr + "'";
                            locationPartitioningString = cols2Partition.get(0)
                                    + "=" + dt1 + "/" + cols2Partition.get(1)
                                    + "=" + hr;

                            String addPartionPE = "ALTER TABLE "
                                    + localTablename
                                    + " ADD IF NOT EXISTS PARTITION ("
                                    + partitioningString + ") LOCATION '"
                                    + locationPartitioningString + "';";
                            addPartitionArray.add(addPartionPE);
                        }
                    }
                } else if (cols2Partition.size() == 3) {

                    String partitioningString = new String();
                    String locationPartitioningString = new String();
                    if (cols2Partition.get(2).equals("rop_id")) {
                        for (int i = 0; i < 24; i++) {

                            //need to change to add for when rops period are generated rather than standard 0,15,30,45
                            List<String> ropList = new ArrayList<String>();
                            ropList.add("00");
                            ropList.add("15");
                            ropList.add("30");
                            ropList.add("45");
                            for (String rop : ropList) {
                                String hr = "";

                                if (i < 10) {
                                    hr = "0" + i;

                                } else {
                                    hr = Integer.toString(i);
                                }
                                partitioningString = cols2Partition.get(0)
                                        + "='" + dt1 + "',"
                                        + cols2Partition.get(1) + "='" + hr
                                        + "'" + "'," + cols2Partition.get(2)
                                        + "='" + rop + "'";
                                locationPartitioningString = cols2Partition
                                        .get(0) + "=" + dt1 + "/"
                                        + cols2Partition.get(1) + "=" + hr
                                        + "'," + cols2Partition.get(2) + "='"
                                        + rop + "'";

                                String addPartionPE = "ALTER TABLE "
                                        + localTablename
                                        + " ADD IF NOT EXISTS PARTITION ("
                                        + partitioningString + ") LOCATION '"
                                        + locationPartitioningString + "';";
                                addPartitionArray.add(addPartionPE);
                            }
                        }
                    }
                }
                for (String sql : addPartitionArray) {
                    impalaExec.executeSQL(sql);                    
                    log.info("Successfully added partition to table "
                            + localTablename + " for " + dt1 + ".");

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Issuse added partition to table " + localTablename
                    + " for " + dt1 + ".");
        }
    }

    private long getElapsedTime() {
        long now = System.currentTimeMillis();
        long elapsedtime = now - start;
        start = now;

        return elapsedtime;
    }

}