package com.blastme.messaging.configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
	private static String pgHost;
	private static int pgPort;
	private static String pgDB;
	private static String pgUser;
	private static String pgPass;
	private static int pgConnPoolSize;
	
	private static String redisHost;
	private static int redisPort;
	private static String redisAuth;
	
	private static String logConfigPath;
	
	private static String timeZone;
	private static double emailBalanceThreshold;
	
	private static String rabbitHost;
	private static String rabbitUser;
	private static String rabbitPass;
	private static String rabbitVirtualHost;
	
	private static String csvTransactionReportPath;
	
	private static String uploadBatchFileDirectory;
	private static String voiceBlastBNumberDirectory;
	
	private static String emailAPIPath;
	private static int emailAPIPort;
	
	private static String emailAttachmentIndexPath;
	
	public Configuration() {
		Properties prop = new Properties();
		InputStream input = null;
		
		try{
			//input = new FileInputStream("D:\\GoogleDrive\\SyncedProjects\\BlastMe\\messaging\\src\\main\\resources\\blastme.properties");
			input = new FileInputStream("/pintar/config/blastme.properties"); 	// BLASTME PRODUCTION
//			input = new FileInputStream("/app/blastme/resources/blastme.properties"); 	// BLASTME PRODUCTION
			//input = new FileInputStream("/artamaya/config/blastme.properties");
			//input = new FileInputStream("/pasti/config/blastme.properties");
			
			prop.load(input);
			
			pgHost = prop.getProperty("pg.host");
			pgPort = Integer.parseInt(prop.getProperty("pg.port"));
			pgDB = prop.getProperty("pg.name");
			pgUser = prop.getProperty("pg.username");
			pgPass = prop.getProperty("pg.password");
			pgConnPoolSize = Integer.parseInt(prop.getProperty("pg.connpoolsize"));
			
			redisHost = prop.getProperty("redis.host");
			redisPort = Integer.parseInt(prop.getProperty("redis.port"));
			redisAuth = prop.getProperty("redis.auth");
			
			logConfigPath = prop.getProperty("log4j2.path");
			
			timeZone = prop.getProperty("system.timezone");
			emailBalanceThreshold = Double.parseDouble(prop.getProperty("system.email.balancethreshold"));
			
			rabbitHost = prop.getProperty("rabbitmq.host");
			rabbitUser = prop.getProperty("rabbitmq.user");
			rabbitPass = prop.getProperty("rabbitmq.pass");
			rabbitVirtualHost = prop.getProperty("rabbitmq.virtualhost");

			csvTransactionReportPath = prop.getProperty("csv.transactionreport");
			
			uploadBatchFileDirectory = prop.getProperty("upload.batchfiledirectory");
			voiceBlastBNumberDirectory = prop.getProperty("upload.voicebnumberdirectory");
			
			emailAPIPath = prop.getProperty("email.api.path");
			emailAPIPort = Integer.parseInt(prop.getProperty("email.api.port"));
			
			emailAttachmentIndexPath = prop.getProperty("system.email.attachmentindexpath");
			
			input.close();
		} catch (IOException e){
			System.out.println("Configuration module is not loaded. Please check the application.");
			System.exit(100);
		}
	}

	public static String getPgHost() {
		return pgHost;
	}

	public static int getPgPort() {
		return pgPort;
	}

	public static String getPgDB() {
		return pgDB;
	}

	public static String getPgUser() {
		return pgUser;
	}

	public static String getPgPass() {
		return pgPass;
	}

	public static int getPgConnPoolSize() {
		return pgConnPoolSize;
	}

	public static String getRedisHost() {
		return redisHost;
	}

	public static int getRedisPort() {
		return redisPort;
	}

	public static String getRedisAuth() {
		return redisAuth;
	}

	public static String getLogConfigPath() {
		return logConfigPath;
	}

	public static String getTimeZone() {
		return timeZone;
	}

	public static double getEmailBalanceThreshold() {
		return emailBalanceThreshold;
	}

	public static String getRabbitHost() {
		return rabbitHost;
	}

	public static String getRabbitUser() {
		return rabbitUser;
	}

	public static String getRabbitPass() {
		return rabbitPass;
	}

	public static String getRabbitVirtualHost() {
		return rabbitVirtualHost;
	}

	public static String getCsvTransactionReportPath() {
		return csvTransactionReportPath;
	}

	public static String getEmailAPIPath() {
		return emailAPIPath;
	}

	public static int getEmailAPIPort() {
		return emailAPIPort;
	}

	public static String getEmailAttachmentIndexPath() {
		return emailAttachmentIndexPath;
	}

	public static String getUploadBatchFileDirectory() {
		return uploadBatchFileDirectory;
	}

	public static String getVoiceBlastBNumberDirectory() {
		return voiceBlastBNumberDirectory;
	}

	
}
