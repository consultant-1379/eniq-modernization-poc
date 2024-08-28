package com.ericsson.kyo.workers;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.kyo.KYO;
import com.ericsson.kyo.mdr.FlowProperties;
import com.ericsson.kyo.mdr.ReportableEntity;
import com.ericsson.kyo.mdr.PostgresParserDataLoader;
import com.ericsson.kyo.mdr.pe2re;
import com.ericsson.kyo.mdr.workers.WorkerBase;
import com.ericsson.kyo.impala.ImpalaRunSQL;

public class CreateRE extends WorkerBase{
	
	private static final Logger log = LoggerFactory
            .getLogger(CreateRE.class);
    private long start = System.currentTimeMillis();
	ImpalaRunSQL impalaExec;
	public CreateRE() {
		super();
		impalaExec = new ImpalaRunSQL();
	}
	
	public void execute() {
		String techpack_version_id=props.getTechPackID();
		log.info("Creating Impala Views for TeckPack"+techpack_version_id);
		String techpack_name_release=mdr.getVersionInfo4TechPack(techpack_version_id);
		String[] TempArray = techpack_name_release.split(":");
		String TP=TempArray[0].trim();
		String Version=TempArray[1].trim();
		ArrayList<String> TPre=(ArrayList<String>) mdr.getReportableEntityNamePerTechPack(TP);
		ArrayList<String> TPreID=(ArrayList<String>) mdr.getReportableEntityName_IDPerTechPack(TP+"%");
		ArrayList<String> TPversions=mdr.getVersions4TechPack(TP);
			
		pe2re pe2re= GenReColStructure(TPreID,TPversions,TP);
    	System.out.printf("Column Structure successfully generated for TechPack %s in %d seconds.\n",TP,getElapsedTime()/1000);
        log.info("Column Structure successfully generated for TechPack %s in %d seconds.\n",TP,getElapsedTime()/1000);
		
        createView(pe2re,TPre,TPversions,TP);
    	System.out.printf("Views successfully created  for TechPack %s in %d seconds.\n",TP,getElapsedTime()/1000);
        log.info("Views successfully created for TechPack %s in %d seconds.\n",TP,getElapsedTime()/1000);

	}
		
	private void createView(pe2re pe2re,ArrayList<String> TPre, ArrayList<String> TPversions, String techpack_name ){
		HashMap<String,ArrayList<String>> map4RE= new HashMap<String,ArrayList<String>>();
		HashMap<String,ArrayList<String>> map4colPerVersion= new HashMap<String,ArrayList<String>>();
	
		map4RE=pe2re.getMap4RE();
		map4colPerVersion=pe2re.getMap4colPerVersion();
		
		for (String RE : TPre) {
			
			String DropView="DROP VIEW IF EXISTS "+RE+";";
			String CreateView="CREATE VIEW IF NOT EXISTS "+RE+" as ";
				ArrayList<String> peColList=map4RE.get(RE);
				for (String VersionCreate : TPversions) {
					String cols4createViewlineperversion= new String();
					String ArrayLsit4createViewlineperversion= " FULL OUTER JOIN ";
					ArrayList<String> peColList4Version=map4colPerVersion.get(RE+"_"+VersionCreate);
					Integer checkAllEqualCount=0;
					for (String  peCol4Version: peColList4Version) {
						if (peCol4Version.contains("NotInVersion")) {
							checkAllEqualCount=checkAllEqualCount+1;
						}
					}
					String createViewlineperversion= new String();
					if (!checkAllEqualCount.equals(peColList.size())) {
						createViewlineperversion="SELECT '"+VersionCreate+"' as version, <replace_String4cols> from "+RE +"_"+VersionCreate+" <replace_String4arrays> UNION ALL ";
						for (String  peCol: peColList) {
							
							String[] TempArray = peCol.split(",");
							String reCol=TempArray[0].trim();
							String reColType=TempArray[1].trim();
							String peColType4Version=new String();
							for (String  peElement4Version: peColList4Version) {
								if (peElement4Version.contains(reCol+',')) {
									peColType4Version=peElement4Version;
								}
							}

							String[] TempArrayPerVersion = peColType4Version.split(",");
							String reColPerVersion=TempArrayPerVersion[0].trim();
							String reColTypeVersion=TempArrayPerVersion[1].trim();
							
							if (reColType.contains("ARRAY<")) {
								if (reColTypeVersion.contains("NotInVersion")){
									cols4createViewlineperversion=cols4createViewlineperversion+" NULL AS "+reColPerVersion+"_index,  NULL AS "+reColPerVersion+",";
								}else {
								cols4createViewlineperversion=cols4createViewlineperversion+reColPerVersion+".pos as "+reColPerVersion+"_index, "+reColPerVersion+".item as "+reColPerVersion+",";
								ArrayLsit4createViewlineperversion=ArrayLsit4createViewlineperversion+RE +"_"+VersionCreate+"."+reColPerVersion+" FULL OUTER JOIN ";	
								}
							}else if (reColTypeVersion.equalsIgnoreCase(reColType)) {
								cols4createViewlineperversion=cols4createViewlineperversion+reColPerVersion+",";
							}else if (reColTypeVersion.contains("NotInVersion")){
								cols4createViewlineperversion=cols4createViewlineperversion+" NULL AS "+reColPerVersion+",";
							}else if (!reColTypeVersion.equalsIgnoreCase(reColType)) {
								if (reColType.equalsIgnoreCase("VARCHAR") && !reColTypeVersion.equalsIgnoreCase("VARCHAR")) {
									cols4createViewlineperversion=cols4createViewlineperversion+" CAST("+reColPerVersion+" AS VARCHAR) "+reColPerVersion+",";
									log.warn("Check type conversion for view: %s, table:%s, column:%s types:view:%s ver:%s.\n",RE,RE+"_"+VersionCreate,reColPerVersion,reColType,reColTypeVersion);
									System.out.printf("Check type conversion for view: %s, table:%s, column:%s types:view:%s ver:%s.\n",RE,RE+"_"+VersionCreate,reColPerVersion,reColType,reColTypeVersion);
								}else if (reColType.equalsIgnoreCase("DOUBLE") && reColTypeVersion.contains("INT")) {
									cols4createViewlineperversion=cols4createViewlineperversion+" CAST("+reColPerVersion+" AS DOUBLE) "+reColPerVersion+",";
								}else if (reColType.equalsIgnoreCase("FLOAT") && reColTypeVersion.contains("INT")) {
									cols4createViewlineperversion=cols4createViewlineperversion+" CAST("+reColPerVersion+" AS FLOAT) "+reColPerVersion+",";
								}else{
									cols4createViewlineperversion=cols4createViewlineperversion+reColPerVersion+",";
									log.warn("Check type conversion for view: %s, table:%s, column:%s types:view:%s ver:%s.\n",RE,RE+"_"+VersionCreate,reColPerVersion,reColType,reColTypeVersion);
									System.out.printf("Check type conversion for view: %s, table:%s, column:%s types:view:%s ver:%s.\n",RE,RE+"_"+VersionCreate,reColPerVersion,reColType,reColTypeVersion);
								}
									
								
							}
						}
						
						createViewlineperversion=createViewlineperversion.replace("<replace_String4cols>", cols4createViewlineperversion.substring(0, cols4createViewlineperversion.length()-1)).replace("<replace_String4arrays>",ArrayLsit4createViewlineperversion.substring(0, ArrayLsit4createViewlineperversion.length()-17));
					}
					if (cols4createViewlineperversion.length()>0) {
						CreateView=CreateView+" \n"+createViewlineperversion;
					}
					

				}
				CreateView=CreateView.substring(0, CreateView.length()-10)+";";
		            	
				impalaExec.executeSQL(DropView);
            	impalaExec.executeSQL(CreateView);
            	//mdr.updateReportableEntity(techpack_name, RE , "View generate for "+techpack_name+ " for versions:"+String.join(",",TPversions ).replace(",null", ""), "VIEW");
            	for (String  peCol: peColList) {
					
					String[] TempArray = peCol.split(",");
					String reCol=TempArray[0].trim();
					String reColType=TempArray[1].trim();
					mdr.updateReportableEntityColumns(techpack_name, RE, reCol, reColType,RE+" column for "+techpack_name+ " for versions:"+String.join(",",TPversions ).replace(",null", ""));
            	}
		}

	}
	
	
	private pe2re GenReColStructure (ArrayList<String> TPreID ,ArrayList<String> TPversions, String TP )  {
		pe2re pe2re= new pe2re();
		HashMap<String,ArrayList<String>> map4RE= new HashMap<String,ArrayList<String>>();
		HashMap<String,ArrayList<String>> map4colPerVersion= new HashMap<String,ArrayList<String>>();
		for (String RE_REID : TPreID) {
			String[] TempArraySplit_RE_REID = RE_REID.split(",");
			String RE=TempArraySplit_RE_REID[0];
			String REID=TempArraySplit_RE_REID[1];
			ArrayList<String> REcolList=(ArrayList<String>) mdr.getColListPerPE(REID,TP+"%");
			ArrayList<String> REcolType=new ArrayList<String>();
			for (String VersionCreate : TPversions) {
				ArrayList<String> impalaColDataTypePerVersion= new ArrayList<String>();
				for (String REcol : REcolList) {
					
					String colDataType=mdr.getColDataTypePerColVersion(RE, TP, VersionCreate,REcol);
					
					if ( colDataType.contains("ARRAY<") ||colDataType.contains("MAP<")) {
    					String[] TempArraySplit4ReCol_Size = colDataType.split(":");
    						colDataType=TempArraySplit4ReCol_Size[0];
    					
					}
					
            		impalaColDataTypePerVersion.add(REcol+","+colDataType);
            		if ( REcolType.isEmpty() ) {
            			REcolType.add(REcol+","+colDataType);
            		}else{
            			Integer index = -1;
            			for (String element :REcolType) {
            				if (element.contains(REcol+",")) {
            					index=REcolType.indexOf(element);
            				}
            			}	
            			if(index.equals(-1)) {
            				REcolType.add(REcol+","+colDataType);
            			}else{
            			           				
        					if ( REcolType.get(index).contains("NotInVersion") && !colDataType.contains("NotInVersion")) {
                				REcolType.set(index, REcol+","+colDataType);
                			}else if (colDataType.contains("DOUBLE")  && REcolType.get(index).contains("INT") ) {
                				if ( REcolType.get(index).contains("ARRAY<BIGINT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("BIGINT", "DOUBLE"));
                				}else if (REcolType.get(index).contains("ARRAY<INT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("INT", "DOUBLE"));
                				}else if ( REcolType.get(index).contains("MAP<BIGINT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("BIGINT", "DOUBLE"));
                				}else if (REcolType.get(index).contains("MAP<INT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("INT", "DOUBLE"));
                				}else {
                					REcolType.set(index, REcol+",DOUBLE");
                				}
                			}else if (colDataType.contains("FLOAT")  && REcolType.get(index).contains("INT") ) {
                				if ( REcolType.get(index).contains("ARRAY<BIGINT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("BIGINT", "FLOAT"));
                				}else if (REcolType.get(index).contains("ARRAY<INT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("INT", "FLOAT"));
                				}else if ( REcolType.get(index).contains("MAP<BIGINT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("BIGINT", "FLOAT"));
                				}else if (REcolType.get(index).contains("MAP<INT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("INT", "FLOAT"));
                				}else {
                					REcolType.set(index, REcol+",FLOAT");
                				}
                			}else if(colDataType.contains("BIGINT")  && REcolType.get(index).equals("INT")) {
                				 if (REcolType.get(index).contains("ARRAY<INT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("INT", "BIGINT"));
                				}else if ( REcolType.get(index).contains("MAP<INT>:")){
                					REcolType.set(index, REcol+","+colDataType.replace("BIGINT", "FLOAT"));
                				}else {
                					REcolType.set(index, REcol+",FLOAT");
                				}
                			}else if(colDataType.contains("VARCHAR")  && !REcolType.get(index).contains("VARCHAR")) {
                				if (REcolType.get(index).contains("INT")){
                					REcolType.set(index, REcol+","+colDataType.replace("INT", "VARCHAR"));
                				}else if (REcolType.get(index).contains("BIGINT")){
                					REcolType.set(index, REcol+","+colDataType.replace("BIGINT", "VARCHAR"));
                				}else if (REcolType.get(index).contains("FLOAT")){
                					REcolType.set(index, REcol+","+colDataType.replace("FLOAT", "VARCHAR"));
                				}else if (REcolType.get(index).contains("DOUBLE")){
                					REcolType.set(index, REcol+","+colDataType.replace("DOUBLE", "VARCHAR"));
                				}
                			}
            			}
			
				}	
			}
			map4RE.put(RE, REcolType);
			map4colPerVersion.put(RE+"_"+VersionCreate, impalaColDataTypePerVersion);
			}
			
		}
		
	pe2re.setMap4RE(map4RE);
	pe2re.setMap4colPerVersion(map4colPerVersion);
	return pe2re;
    }
         
    private long getElapsedTime() {
        long now = System.currentTimeMillis();
        long elapsedtime = now - start;
        start = now;

        return elapsedtime;
    }
}

