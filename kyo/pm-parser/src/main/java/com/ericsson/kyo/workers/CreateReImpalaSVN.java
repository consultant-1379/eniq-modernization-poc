package com.ericsson.kyo.workers;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.KYO;
import com.ericsson.kyo.mdr.*;
import com.ericsson.kyo.mdr.workers.WorkerBase;
import com.ericsson.kyo.postgres.PostgresHotStore;
import com.ericsson.kyo.impala.ImpalaRunSQL;

public class CreateReImpalaSVN extends WorkerBase{
	
	private static final Logger log = LoggerFactory
            .getLogger(CreateReImpalaSVN.class);
    private long start = System.currentTimeMillis();
	ImpalaRunSQL impalaExec;
	PostgresHotStore postgresExec;
	public CreateReImpalaSVN() {
		super();
		impalaExec = new ImpalaRunSQL();
		postgresExec = new PostgresHotStore();
	}
	
	public void execute() {
		String techpack_version_id=props.getTechPackID();
		log.info("Creating Impala tables for TeckPack Version ID:"+techpack_version_id);
		String TechPackDetails =  mdr.getTechPackDetails(techpack_version_id);	
	
		ArrayList<String> reportable_entity_names_ID =  (ArrayList<String>) mdr.getReportableEntityName_IDPerTechPack(techpack_version_id)	;	
		createREimpala(techpack_version_id, reportable_entity_names_ID,TechPackDetails);
		 }
		
		    
	public void createREimpala(String techpack_version_id, ArrayList<String> reportable_entity_names_ID, String TechPackDetails )  {
    	int cnt = 1;
    	String[] TempArray1 = TechPackDetails.split(",");
    	String techpack_name=TempArray1[0].trim();
    	String techpack_release=TempArray1[1].trim();
    	String techpack_release_norm=techpack_release.replace(".", "_");
    	/*hard coding now till updated correctly to give number per parser*/
    	int numSparkWorkEnv=2;
    	//Properties worker_config = mdr.getWorkerConfig(techpack_version_id, techpack_name, "com.ericsson.kyo.workers.PMFileParser");
    	//String MagicNumStr =worker_config.getProperty("MagicNum");
    	
    	
    	
    	
		try {
        	for (String RE_ID : reportable_entity_names_ID) {
        		
        		String[] TempArray = RE_ID.split(",");
				String RE=TempArray[0].trim();
				String reID=TempArray[1].trim();
				String retention_plan="DAY";
				String partition_plan="SHORTTERM";
				String RE_match=RE.substring(0, RE.length() - 1);
				Pattern p = Pattern.compile("_\\d+$");
        		Matcher m = p.matcher(RE);
        		int TableSplitNum=0;
        		if(m.find()) {
        			
        			TableSplitNum=mdr.getNumTablesSplit(RE_match,techpack_version_id);
        		}
        			
				
				String  Retention_Partition_plan= mdr.getRetention_Partition_plan(RE,reID);
				String[] TempArray_Retention_Partition_plan = Retention_Partition_plan.split(",");              
				retention_plan=TempArray_Retention_Partition_plan[0].trim();
				partition_plan=TempArray_Retention_Partition_plan[1].trim();

				
				HashMap<String, ReportableEntity>  map2createRE =  mdr.getREcolumnDetails(reID,techpack_version_id);
            
            for (Entry<String, ReportableEntity> entry : map2createRE.entrySet()) {
		    	ReportableEntity entity = entry.getValue(); 
		    	ArrayList<String> Columns = entity.getaltColumns();
            	ArrayList<String> ColumnDataTypes = entity.getColumnDataTypes();
            	ArrayList<Integer> ParitionbyFlags = entity.getParitionbyFlags();
            	String ColumnlistStringImpala="(";
            	String PartBYlistStringImpala="(";
            	String ColumnlistStringPostgres="(";
            	
            	for (int i = 0; i < Columns.size(); i++) {
            		String ColumnType=new String();
            		if (ColumnDataTypes.get(i).contains("ARRAY<")) {
            			String[] TempArray2 = ColumnDataTypes.get(i).split(":");
            			ColumnType=TempArray2[0];
            		}else if (ColumnDataTypes.get(i).contains("MAP<")) {
            			String[] TempArray2 = ColumnDataTypes.get(i).split(":");
            			ColumnType=TempArray2[0];
            		}else {
            			ColumnType=ColumnDataTypes.get(i);
            		}
            		
            		
            		
            		if (ParitionbyFlags.get(i).equals(0) ) {	
            			ColumnlistStringImpala=ColumnlistStringImpala+" "+Columns.get(i)+" "+ColumnType+",";	
            		}
            		if (ParitionbyFlags.get(i).equals(1) ) {	
            			PartBYlistStringImpala=PartBYlistStringImpala+" "+Columns.get(i)+" "+ColumnType+",";	
            		}
            		
            		String PostgresColumnDataType="";
            		if (ColumnType.equals("DOUBLE") || ColumnType.equals("REAL")){
            			PostgresColumnDataType="DOUBLE PRECISION";
            		} else if(ColumnType.equals("FLOAT")) {
            			PostgresColumnDataType="REAL";
            		}else if(ColumnType.equals("INT")) {
            			PostgresColumnDataType="INTEGER";
            		}else if(ColumnType.equals("TINYINT")) {
            			PostgresColumnDataType="SMALLINT";
            		}else if(ColumnType.equals("VARCHAR") || ColumnType.equals("CHAR") || ColumnType.equals("STRING") ) {
            			PostgresColumnDataType="VARCHAR";
            		}else {
            			PostgresColumnDataType=ColumnType;
            		}
            		
            		if (PostgresColumnDataType.contains("ARRAY<") ) {
            			PostgresColumnDataType=PostgresColumnDataType.replace("ARRAY<", "").replace(">", "")+" []";
            		}
            		
            		ColumnlistStringPostgres=ColumnlistStringPostgres+" "+Columns.get(i)+" "+PostgresColumnDataType+",";
            	}
            	
            	
            	
            	ColumnlistStringImpala=ColumnlistStringImpala.substring(0,ColumnlistStringImpala.length() - 1)+")";
            	PartBYlistStringImpala=PartBYlistStringImpala.substring(0,PartBYlistStringImpala.length() - 1)+")";
            	String baselocation = "/"+techpack_name.replace('_','/')+"/"+techpack_release_norm;
            	String impalaTableName=RE+"_"+techpack_release;
            	String location=baselocation + "/" + RE;
            	m = p.matcher(RE);
            	if(m.find()) {
            		impalaTableName=RE.replaceAll("_\\d+$", "")+"_"+techpack_release;
            		location=location.replaceAll("_\\d+$", "");
        		}
        		
            	
            	String CreatePE = "CREATE EXTERNAL TABLE " +impalaTableName+ " " + ColumnlistStringImpala + " PARTITIONED BY " + PartBYlistStringImpala + " STORED AS PARQUET LOCATION '" + location + "' ;";
            	String DropPE = "DROP TABLE IF EXISTS " + impalaTableName+" ;";   
              	
            	impalaExec.executeSQL(DropPE);
            	impalaExec.executeSQL(CreatePE);
            	 
            	ColumnlistStringPostgres=ColumnlistStringPostgres.substring(0,ColumnlistStringPostgres.length() - 1)+")";
            	String CreatePEPostgresView = "CREATE OR REPLACE VIEW " + RE_match+techpack_release+"_HS_RAW AS SELECT * FROM";
            	String HSTInsertArray=""; 
            	if ( TableSplitNum>0) {
            		for (int num=0; num<TableSplitNum;num++) {
            			for (int numSWE=1; numSWE<=numSparkWorkEnv;numSWE++) {
            				
	                    	String CreatePEPostgres = "CREATE TABLE IF NOT EXISTS " + RE_match +num+"_SWE"+numSWE+"_"+techpack_release+ " " + ColumnlistStringPostgres +";";
	                    	CreatePEPostgresView=CreatePEPostgresView+" "+ RE_match +num+"_SWE"+numSWE+"_"+techpack_release+" NATURAL FULL JOIN ";
	                    	postgresExec.executeWriteSQLResult(CreatePEPostgres);	
	                    	mdr.updateReportableEntity(techpack_name+"_"+techpack_release, RE_match +num+"_SWE"+numSWE+"_"+techpack_release , "Table generated for "+techpack_name+ " for version: "+techpack_release+" for SWE:"+numSWE, "HST",partition_plan,retention_plan,"{}");
	                    	HSTInsertArray=HSTInsertArray+","+RE_match +num+"_SWE"+numSWE+"_"+techpack_release;
            			}
                	}
            		
                	HSTInsertArray=HSTInsertArray.substring(1, HSTInsertArray.length());
                	HSTInsertArray="{"+HSTInsertArray+"}";
                	
                	CreatePEPostgresView=CreatePEPostgresView.substring(0, CreatePEPostgresView.length()-19)+";";

                	postgresExec.executeWriteSQLResult(CreatePEPostgresView);
                	mdr.updateReportableEntity(techpack_name+"_"+techpack_release, RE_match+techpack_release+"_HS_RAW" , "View generated for "+techpack_name+ " to combine HST.", "HSV",partition_plan,retention_plan,HSTInsertArray);          
            	}else {
       
                	String CreatePEPostgres = "CREATE TABLE IF NOT EXISTS " + RE +"_"+techpack_release+ " " + ColumnlistStringPostgres +";";
                	postgresExec.executeWriteSQLResult(CreatePEPostgres);	
            	}
            	
            	 
            	log.info("Succesfully created Table in Impala and VIEW in Postgres Hot Store for "+RE+"_"+techpack_release+" for TechPack "+techpack_name+" Version "+techpack_release+": "+cnt+" of "+reportable_entity_names_ID.size());	
            	
            
            	cnt=cnt+1;		
        	}
            	
            } }catch (Exception e) {
        	log.warn("Error running SQL to Impala to create table  for TechPack "+techpack_version_id+" "+e);
        }
		
        log.info("Tables successfully created for TechPack "+techpack_version_id+" in "+getElapsedTime()/1000+" seconds.\n");
      
    }
         
    private long getElapsedTime() {
        long now = System.currentTimeMillis();
        long elapsedtime = now - start;
        start = now;

        return elapsedtime;
    }
}

