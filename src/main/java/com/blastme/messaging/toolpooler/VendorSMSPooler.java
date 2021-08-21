package com.blastme.messaging.toolpooler;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;
import com.blastme.messaging.configuration.Configuration;

public class VendorSMSPooler {
	private static Logger logger;
	public static JSONObject jsonVendorSMSProperty;
	
	private Connection connection = null;
	private PreparedStatement statement = null;
    private ResultSet resultSet = null;

	public VendorSMSPooler() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("POOLER");

		// Initiate LoggingPooler
		new LoggingPooler();

		// Initiate connection to Postgresql
        try {
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "VendorSMSPooler", "VendorSMSPooler", false, false, false, "", 
    				"Database connection is load and initiated.", null);
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VendorSMSPooler", "VendorSMSPooler", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
        }
        
		// Initate JSONClientProperty
		jsonVendorSMSProperty = new JSONObject();
		initiateJSONVendorSMSProperty();
		
		LoggingPooler.doLog(logger, "INFO", "VendorSMSPooler", "VendorSMSPooler", false, false, false, "", 
				"Module VendorSMSPooler is initiated and ready to serve. jsonVendorSMSProperty: " + jsonVendorSMSProperty.toString(), null);
	}

	private void initiateJSONVendorSMSProperty(){
		// Query to Postgresql
		try{
            statement = connection
                    .prepareStatement("select vendor_id, vendor_name, queue_name from vendor_sms where is_active = true");
            resultSet = statement.executeQuery();

            while(resultSet.next()){
            	JSONObject jsonDetail = new JSONObject();
            	
    			jsonDetail.put("vendorId", resultSet.getString("vendor_id"));
    			jsonDetail.put("vendorName", resultSet.getString("vendor_name"));
    			jsonDetail.put("queueName", resultSet.getString("queue_name"));
            	
            	jsonVendorSMSProperty.put(resultSet.getString("vendor_id").trim(), jsonDetail);
            }
            
    		LoggingPooler.doLog(logger, "DEBUG", "VendorSMSPooler", "initiateJSONVendorSMSProperty", false, false, false, "", 
    				"jsonVendorSMSProperty: " + jsonVendorSMSProperty.toString(), null);
		} catch (Exception e) {
			e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VendorSMSPooler", "initiateJSONVendorSMSProperty", true, false, false, "", 
    				"Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
		} finally {
			 try {
	                if (resultSet != null)
	                    resultSet.close();
	                if (statement != null)
	                    statement.close();
	                if (connection != null)
	                	connection.close();
			 } catch(Exception e) {
					e.printStackTrace();
		    		LoggingPooler.doLog(logger, "DEBUG", "VendorSMSPooler", "initiateJSONVendorSMSProperty", true, false, false, "", 
		    				"Failed to close query statement.", e);
			 }
		}		
		
		System.out.println("jsonVendorSMSProperty: " + jsonVendorSMSProperty.toString());
	}
}
