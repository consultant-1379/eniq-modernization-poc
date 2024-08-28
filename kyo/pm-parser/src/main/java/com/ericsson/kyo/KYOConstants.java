package com.ericsson.kyo;

public class KYOConstants {

	public static String Dir_to_listen = System.getProperty("Dir_to_listen", "C:/Users/ebrifol/Dev/KYO/scratchpad/DataSource");
	public static String[] Node_of_interest = {"IngressDir"};
	public static String Dir_to_write = System.getProperty("Dir_to_write", "C:/Users/ebrifol/Dev/KYO/scratchpad/Output");
	public static String ser_path = System.getProperty("ser_path", "C:\\Users\\ebrifol\\Dev\\KYO\\scratchpad\\ser");
	public static String queries_file = System.getProperty("queries_file", "C:\\Users\\ebrifol\\Dev\\KYO\\scratchpad\\queries.txt");
	
	
	//Spark related properties
	public static String SPARK_MASTER = System.getProperty("Spark_master", "local");
	public static String SPARK_APPNAME = System.getProperty("Spark_appName", "KYO");
	public static int SPARK_PORT = Integer.valueOf( System.getProperty("Spark_port", "3000") );
	
	
	
}
