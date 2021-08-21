package com.blastme.messaging.toolpooler;

import org.apache.commons.dbcp2.BasicDataSource;

import com.blastme.messaging.configuration.Configuration;

public class DataSource {
	private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
    //private static final String DB_URL = "jdbc:postgresql://localhost:5432/BlastMe";
	//private static final String DB_URL = "jdbc:postgresql://localhost:5432/blastme"; // Production
	private static String DB_URL;
    //private static final String DB_USER = "eliandrie";
    //private static final String DB_USER = "pawitra";  // Production
	private static String DB_USER;
    private static String DB_PASSWORD;
    private static int CONN_POOL_SIZE;
 
    private BasicDataSource bds = new BasicDataSource();
    
	public DataSource() {
		// Initiate value
		new Configuration();
		
		DB_URL = "jdbc:postgresql://" + Configuration.getPgHost() + ":" + Integer.toString(Configuration.getPgPort()) + "/" + Configuration.getPgDB();
		DB_USER = Configuration.getPgUser().trim();
		DB_PASSWORD = Configuration.getPgPass().trim();
		CONN_POOL_SIZE = Configuration.getPgConnPoolSize();
		
		//Set database driver name
        bds.setDriverClassName(DRIVER_CLASS_NAME);
        //Set database url
        bds.setUrl(DB_URL);
        //Set database user
        bds.setUsername(DB_USER);
        //Set database password
        bds.setPassword(DB_PASSWORD);
        //Set the connection pool size
        bds.setInitialSize(CONN_POOL_SIZE);
        
        bds.setMaxTotal(100);
        bds.setMaxOpenPreparedStatements(100);
        bds.setMinIdle(0);
        bds.setMaxIdle(10);
	}
	
	private static class DataSourceHolder {
        private static final DataSource INSTANCE = new DataSource();
    }
 
    public static DataSource getInstance() {
        return DataSourceHolder.INSTANCE;
    }
 
    public BasicDataSource getBds() {
        return bds;
    }
 
    public void setBds(BasicDataSource bds) {
        this.bds = bds;
    }
}
