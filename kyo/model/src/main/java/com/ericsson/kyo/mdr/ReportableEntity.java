package com.ericsson.kyo.mdr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class ReportableEntity {

	private String MOname;
	private String Version;
	private String Tablename;
	private String Loc_Par;
	private String TP;
	private String Retention_Plan;
	private Integer Retention_Period;
	private ArrayList<String> alt_columns;
	private LinkedHashMap<String, List<String>> columns;
	private ArrayList<String> paritionby;
	private ArrayList<Integer> paritionbyFlags;
	private ArrayList<String> columnDataTypes;
	private ArrayList<String> columnCounterTypes;
	private ArrayList<String> mappedMOs;
	
	public ReportableEntity() {
		columns = new LinkedHashMap<String, List<String>>();
		alt_columns = new ArrayList<String>();
		paritionby = new ArrayList<String>();
		paritionbyFlags = new ArrayList<Integer>();
		columnDataTypes = new ArrayList<String>();
		columnCounterTypes = new ArrayList<String>();
		mappedMOs = new ArrayList<String>();
	}
	
	public String getVersion() {
		return Version;
	}
	
	public void setVersion(String version) {
		Version = version;
	}
	
	public String getMOname() {
		return MOname;
	}
	
	public void setMOname(String mOname) {
		MOname = mOname;
	}
	
	public String getTablename() {
		return Tablename;
	}
	
	public void setTablename(String tablename) {
		Tablename = tablename;
	}
	
	public ArrayList<String> getColumnDataTypes() {
		return columnDataTypes;
	}
	
	public void addColumnDataTypes(String columnDataType) {
		this.columnDataTypes.add(columnDataType);
	}
	
	public ArrayList<String> getColumnCounterTypes() {
		return columnCounterTypes;
	}
	
	public void addColumnCounterTypes(String columnCounterType) {
		this.columnCounterTypes.add(columnCounterType);
	}
	
	public ArrayList<String> getMappedMOs(){
		return mappedMOs;
	}
	
	public void addMappedMOs(String[] MOs) {
		mappedMOs.addAll(Arrays.asList(MOs));
	}
	
	public LinkedHashMap<String, List<String>> getColumns() {
		return columns;
	}
	
	public ArrayList<String> getaltColumns(){
		return alt_columns;
	}
	
	public void addColumn(String column) {
		alt_columns.add(column);
	}
	
	public void addColumn(String type, String column) {
		List<String> list_of_columns;
	    if (this.columns.containsKey(type)) {
	      list_of_columns = (List)this.columns.get(type);
	    } else {
	      list_of_columns = new ArrayList();
	    }
	    list_of_columns.add(column);
	    this.columns.put(type, list_of_columns);
	}
	public ArrayList<String> getParitionby() {
		return paritionby;
	}
	
	public void addParitionby(String paritionby) {
		this.paritionby.add(paritionby);
	}
	public ArrayList<Integer> getParitionbyFlags() {
		return paritionbyFlags;
	}
	
	public void addParitionbyFlag(Integer paritionbyFlag) {
		this.paritionbyFlags.add(paritionbyFlag);
	}
	
	public String getLoc_Par() {
		return Loc_Par;
	}
	
	public void setLoc_Par(String loc_par) {
		Loc_Par = loc_par;
	}
	public String getTP() {
		return TP;
	}
	
	public void setTP(String tp) {
		TP = tp;
	}
	
	public String getRetentionPlan() {
		return Retention_Plan;
	}
	
	public void setRetentionPlan(String retention_plan) {
		Retention_Plan = retention_plan;
	}

	public Integer getRetentionPeriod() {
		return Retention_Period;
	}
	
	public void setRetentionPeriod(Integer retention_period) {
		Retention_Period = retention_period;
	}
	
}
