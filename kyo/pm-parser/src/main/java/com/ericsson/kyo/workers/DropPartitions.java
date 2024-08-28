package com.ericsson.kyo.workers;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.KYO;
import com.ericsson.kyo.mdr.FlowProperties;
import com.ericsson.kyo.mdr.ReportableEntity;
import com.ericsson.kyo.mdr.workers.WorkerBase;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.impala.ImpalaRunSQL;




public class DropPartitions extends WorkerBase{
	
	private static final Logger log = LoggerFactory.getLogger(DropPartitions.class);
    private long start = System.currentTimeMillis();
    ImpalaRunSQL impalaExec = new ImpalaRunSQL();
	public void execute() {
		log.info("Executing KYO Partitioning");
				
		HashMap<String, ReportableEntity>  map4Partitioning =  mdr.getEntities4Partitioning();
		
		if(map4Partitioning.size() > 0) {
		
		for (Map.Entry<String, ReportableEntity> entry : map4Partitioning.entrySet()) {
			ReportableEntity entity = entry.getValue();
			String RE = entity.getTablename();
			Pattern p = Pattern.compile("_\\d+$");
    		Matcher m = p.matcher(RE);
    		if(m.find()) {
        		RE=RE.replaceAll("_\\d+$", "");
    		}
	    	String RE_ID=entity.getMOname();
	    	String TP = entity.getTP().replace(".", "_");
	    	String loc_par = entity.getLoc_Par().replace(".", "_");
	    	String retentionPlan=entity.getRetentionPlan();
	    	Integer retentionPeriod=entity.getRetentionPeriod();
	    	String version=entity.getVersion().replace(".", "_");
	    	
	    	
	    	
	    	removePartition(RE,TP,RE_ID,loc_par,retentionPlan,retentionPeriod,version);

		}
    	
	    	
		log.info("Partitioning successfully completed in "+getElapsedTime()/1000+" seconds.");
		
		    
		        }
		
		    }
	
	public void execute(String TechPackName ) {
		log.info("Executing KYO Partitioning");
				
		HashMap<String, ReportableEntity>  map4Partitioning =  mdr.getEntities4Partitioning(TechPackName);
		
		if(map4Partitioning.size() > 0) {
		
		for (Map.Entry<String, ReportableEntity> entry : map4Partitioning.entrySet()) {
			ReportableEntity entity = entry.getValue();
			String RE = entity.getTablename();
			Pattern p = Pattern.compile("_\\d+$");
    		Matcher m = p.matcher(RE);
    		if(m.find()) {
        		RE=RE.replaceAll("_\\d+$", "");
    		}
	    	String RE_ID=entity.getMOname();
	    	String TP = entity.getTP().replace(".", "_");
	    	String loc_par = entity.getLoc_Par().replace(".", "_");
	    	String retentionPlan=entity.getRetentionPlan();
	    	Integer retentionPeriod=entity.getRetentionPeriod();
	    	String version=entity.getVersion().replace(".", "_");
	    	
	    	
	    	
	    	removePartition(RE,TP,RE_ID,loc_par,retentionPlan,retentionPeriod,version);

		}
    	
	    	
		log.info("Partitioning successfully completed in "+getElapsedTime()/1000+" seconds.");
		
		    
		        }
		
		    }
	 private void removePartition(String rE, String tP,String rE_ID, String loc_par,String retentionPlan, Integer retentionPeriod, String version)  {
		LocalDate dtOrg = LocalDate.now();
		LocalDate dtMinusRetention = dtOrg.plusDays(-retentionPeriod);
		String dt1=dtMinusRetention.format(DateTimeFormatter.ofPattern("yyyyMMdd"));  
		
		String localTablename= rE +"_"+version;;
   
		try {
	    	String getPartitionInfoSQL="show partitions "+localTablename+";";
	    	ArrayList<String> Fulllocationlist = impalaExec.getPartitionListImpala(getPartitionInfoSQL);
	    	List<LocalDate> partitionDatelist = new ArrayList<LocalDate>();
	    	List<String> locationlist = new ArrayList<String>();
	    	List<String> partitionlist = new ArrayList<String>();
	    	DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
	    	LocalDate partitionDate = null; 
	        for(String localLocation:Fulllocationlist) {
	        	
	        	if (localLocation.contains("date_id=")){
	        		
	        		Pattern p = Pattern.compile("\\w+/date_id=(\\d+)");
	        		Matcher m = p.matcher(localLocation);
	        		if(m.find()) {
	        			MatchResult mr=m.toMatchResult();
	        			partitionDate= LocalDate.parse(mr.group(1),inputFormat);
	        			if (partitionDate.isBefore(dtMinusRetention)){
	            			partitionDatelist.add(partitionDate);
	            			partitionlist.add(localTablename);
	            			locationlist.add(localLocation);
	            			
	            		}	

	        		}
	        	}
	        	
	        	
	        }
	        if (partitionlist.size()>0) {
		        for (int i=0;i<partitionlist.size();i++) {
		        	String dropImpalaSQL=null;
		        	String localPartitiondate=partitionDatelist.get(i).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		        	
		        	dropImpalaSQL="ALTER TABLE "+partitionlist.get(i)+" DROP IF EXISTS PARTITION (date_id='"+localPartitiondate+"') PURGE;";
		        	
		        	
		        	impalaExec.executeSQL(dropImpalaSQL);

		        	String oldPartitionlocalLocation=locationlist.get(i);
	
	
		        	//delete using java not working from eclipse
		        	boolean status=deleteFileFromHDFS(oldPartitionlocalLocation);
	
		        	if (status) {
		        		log.info("Rentention Plan:"+retentionPlan+" - Successfully dropped partition "+localPartitiondate+" for table "+partitionlist.get(i)+" at location "+oldPartitionlocalLocation+".");
			        	
			            }else {
			            	log.info("Rentention Plan:"+retentionPlan+" - Failed to  drop partition "+localPartitiondate+" for table "+partitionlist.get(i)+" at location "+oldPartitionlocalLocation+".");
			            } 	
		        }
	        }else {
	        	log.info("Rentention Plan:"+retentionPlan+" - No partitions to drop for "+localTablename+" for dates earlier than "+dt1);
	        }
	        
	   }catch (Exception e){
		   log.error("Error running commands to remove partition for "+localTablename+" for date "+dt1+":"+ e);
		e.printStackTrace();
    	}
		
		
		
	}
	 
	private long getElapsedTime() {
	        long now = System.currentTimeMillis();
	        long elapsedtime = now - start;
	        start = now;

	        return elapsedtime;
	    }
	    
	    /* Delete a file or directory from HDFS */
    public static boolean deleteFileFromHDFS(String pathString)
      throws IOException {
      
     Configuration conf = new Configuration();
     //need to update paths to config depending on deployment
     conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
     conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));

     FileSystem  hdfs = null;
     boolean status = false;
     try {
   
      conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
      hdfs = FileSystem.get(new URI("maprfs:"), conf);
      Path path = new Path(pathString.replace("maprfs:", ""));
      //hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
      //Path path = new Path(pathString.replace("hdfs://hadoop:8020", ""));
      if (hdfs.exists(path)) {
    	  
       status = hdfs.delete(path, true);
       
      } else {
    	  log.warn("File does not exist on HDFS");
       status = false;
      }

     } catch (Exception e) {
      e.printStackTrace();
     } finally {
      if (hdfs != null)
       hdfs.close();
     }
     return status;
    }
	    

	}