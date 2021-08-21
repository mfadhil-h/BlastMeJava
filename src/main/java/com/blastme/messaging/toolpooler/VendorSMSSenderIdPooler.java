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

public class VendorSMSSenderIdPooler {
	private static Logger logger;
	public static JSONObject jsonVendorSMSSenderIdProperty;

	public VendorSMSSenderIdPooler() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("POOLER");

		// Initiate LoggingPooler
		new LoggingPooler();

		// Initate JSONClientProperty
		jsonVendorSMSSenderIdProperty = new JSONObject();
		initiateJSONVendorSMSSenderIdProperty();
		
		LoggingPooler.doLog(logger, "INFO", "VendorSMSSenderIdPooler", "VendorSMSSenderIdPooler", false, false, false, "", 
				"Module VendorSMSPooler is initiated and ready to serve. jsonVendorSMSSenderIdProperty: " + jsonVendorSMSSenderIdProperty.toString(), null);
	}

	private void initiateJSONVendorSMSSenderIdProperty(){
		// Query to Postgresql
		Connection connection = null;
		PreparedStatement statement = null;
	    ResultSet resultSet = null;

		try{
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            statement = connection
                    .prepareStatement("select vendor_sender_id_id, sender_id from vendor_senderid_sms where is_active = true");
            resultSet = statement.executeQuery();

            while(resultSet.next()){
            	JSONObject jsonDetail = new JSONObject();
            	
    			jsonDetail.put("id", resultSet.getString("vendor_sender_id_id"));
    			jsonDetail.put("vendorSenderId", resultSet.getString("sender_id"));
            	
            	jsonVendorSMSSenderIdProperty.put(resultSet.getString("vendor_sender_id_id").trim(), jsonDetail);
            }
            
    		LoggingPooler.doLog(logger, "DEBUG", "VendorSMSSenderIdPooler", "initiateJSONVendorSMSSenderIdProperty", false, false, false, "", 
    				"jsonVendorSMSSenderIdProperty: " + jsonVendorSMSSenderIdProperty.toString(), null);
		} catch (Exception e) {
			e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VendorSMSSenderIdPooler", "initiateJSONVendorSMSSenderIdProperty", true, false, false, "", 
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
		    		LoggingPooler.doLog(logger, "DEBUG", "VendorSMSSenderIdPooler", "initiateJSONVendorSMSSenderIdProperty", true, false, false, "", 
		    				"Failed to close query statement.", e);
			 }
		}
	}
}
