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

public class UserAPIHTTPSMSPooler {
	private static Logger logger;
	
	private Connection connection = null;
	private PreparedStatement statement = null;
    private ResultSet resultSet = null;

	// Penampung SystemId and Password and IPAddress
	public static JSONObject jsonHTTPAPIUser;
	
	public UserAPIHTTPSMSPooler() {
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
    		LoggingPooler.doLog(logger, "INFO", "UserAPIHTTPSMSPooler", "UserAPIHTTPSMSPooler", false, false, false, "", 
    				"Database connection is load and initiated.", null);
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "UserAPIHTTPSMSPooler", "UserAPIHTTPSMSPooler", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
        }

		// Load jsonSysIdAccess
		loadJSONSMPPSysId();
		
		LoggingPooler.doLog(logger, "INFO", "UserAPIHTTPSMSPooler", "UserAPIHTTPSMSPooler", false, false, false, "", 
				"Module UserAPIHTTPSMSPooler is initiated and ready to serve. jsonHTTPAPIUser: " + jsonHTTPAPIUser.toString(), null);						
	}

	public void loadJSONSMPPSysId(){
		jsonHTTPAPIUser = new JSONObject();
		
		try{
            statement = connection
                    .prepareStatement("select username, password, client_id, registered_ip_address from user_api where access_type = 'HTTPSMS' and is_active = true");
            resultSet = statement.executeQuery();

            while(resultSet.next()){
            	JSONObject jsonDetail = new JSONObject();
            	
    			jsonDetail.put("sysId", resultSet.getString("username"));
    			jsonDetail.put("password", resultSet.getString("password"));
    			jsonDetail.put("ipAddress", resultSet.getString("registered_ip_address"));
    			jsonDetail.put("clientId", resultSet.getString("client_id"));
    			
    			jsonHTTPAPIUser.put(resultSet.getString("username"), jsonDetail);
            }
            
    		LoggingPooler.doLog(logger, "DEBUG", "UserAPIHTTPSMSPooler", "loadJsonHTTPAPIUser", false, false, false, "", 
    				"jsonHTTPAPIUser: " + jsonHTTPAPIUser.toString(), null);
		} catch (Exception e) {
			e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "UserAPIHTTPSMSPooler", "loadJsonHTTPAPIUser", true, false, false, "", 
    				"Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
		} finally {
			 try {
	                if (resultSet != null)
	                    resultSet.close();
	                if (statement != null)
	                    statement.close();
			 } catch(Exception e) {
					e.printStackTrace();
		    		LoggingPooler.doLog(logger, "DEBUG", "UserAPIHTTPSMSPooler", "loadJsonHTTPAPIUser", true, false, false, "", 
		    				"Failed to close query statement.", e);
			 }
		}		
	}
	
	public JSONObject isValidAPIUser(String username, String password, String ipAddress){
		JSONObject jsonReturn = new JSONObject();
		
		if(jsonHTTPAPIUser.has(username.trim())){
			// systemId valid, check password
			JSONObject jsonDetail = jsonHTTPAPIUser.getJSONObject(username.trim());
			
			if(password.equals(jsonDetail.get("password"))){
				// Password valid, check IPAddress
				String theRegisteredIPAddress = jsonDetail.getString("ipAddress");
				
				if(!theRegisteredIPAddress.trim().equals("ALL")){
					// Registered certain IP, split them by comma
					String[] splittedIpAddress = theRegisteredIPAddress.split(",");
					
					for(int oh = 0; oh < splittedIpAddress.length; oh++){
						if(ipAddress.trim().equals(splittedIpAddress[oh].trim())){
							jsonReturn.put("status", 0); // Success
							jsonReturn.put("description", "Validation success.");
							
							break;
						} else {
							jsonReturn.put("status", -3);
							jsonReturn.put("description", "Unregistered IP Address.");							
						}
					}
				} else {
					// Open for ALL
					jsonReturn.put("status", 0);
					jsonReturn.put("description", "Validation success.");
				}
			} else {
				jsonReturn.put("status", -2);
				jsonReturn.put("description", "Bad password.");
			}
		} else {
			jsonReturn.put("status", -1);
			jsonReturn.put("description", "Bad username.");
		}
		
		return jsonReturn;
	}
	
	public String getClientId(String username){
		String clientId = "";
		
		if(jsonHTTPAPIUser.has(username.trim())){
			JSONObject jsonDetail = jsonHTTPAPIUser.getJSONObject(username.trim());
			
			clientId = jsonDetail.getString("clientId");
		}
		
		return clientId;
	}
}
