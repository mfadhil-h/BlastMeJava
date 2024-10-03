package com.blastme.messaging.toolpooler;

import com.blastme.messaging.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class UserAPISMPPSMSPooler {
    private static Logger logger;

    // Penampung SystemId and Password and IPAddress
    public static JSONObject jsonSysIdAccess;
    public static JSONObject jsonNeoSysIdAccess;

    public static JSONObject jsonClientIdToAccess;
    public static JSONObject jsonNeoClientIdToAccess;

    public UserAPISMPPSMSPooler() {
        // Load Configuration
        new Configuration();
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//		File file = new File(Configuration.getLogConfigPath());
//		context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Load jsonSysIdAccess
        loadJSONSMPPSysId();

        // Load jsonNeoSysIdAddcess
        loadJSONNeoSMPPSysId();

        LoggingPooler.doLog(logger, "INFO", "UserAPISMPPSMSPooler", "UserAPISMPPSMSPooler", false, false, false, "",
                "Module UserAPISMPPSMSPooler is initiated and ready to serve. jsonSysIdAccess: " + jsonSysIdAccess.toString(), null);
    }

    public static void loadJSONSMPPSysId() {
        jsonSysIdAccess = new JSONObject();
        jsonClientIdToAccess = new JSONObject();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();

            String query = "select username, password, client_id, registered_ip_address from user_api where access_type = 'SMPPSMS' and is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("sysId", resultSet.getString("username"));
                jsonDetail.put("password", resultSet.getString("password"));
                jsonDetail.put("ipAddress", resultSet.getString("registered_ip_address"));
                jsonDetail.put("clientId", resultSet.getString("client_id"));

                jsonSysIdAccess.put(resultSet.getString("username"), jsonDetail);

                jsonClientIdToAccess.put(resultSet.getString("client_id"), jsonDetail);
            }

            // Log loaded print large data no needed
//    		LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
//    				"jsonSysIdAccess: " + jsonSysIdAccess.toString(), null);
            LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
                    "jsonSysIdAccess and jsonClientIdToAccess are initiated and ready", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public static void loadJSONNeoSMPPSysId() {
        jsonNeoSysIdAccess = new JSONObject();
        jsonNeoClientIdToAccess = new JSONObject();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();

            String query = "select username, password, client_id, registered_ip_address from user_api where access_type = 'NEOSMPPSMS' and is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("sysId", resultSet.getString("username"));
                jsonDetail.put("password", resultSet.getString("password"));
                jsonDetail.put("ipAddress", resultSet.getString("registered_ip_address"));
                jsonDetail.put("clientId", resultSet.getString("client_id"));
                System.out.println(resultSet.getString("username"));

                jsonNeoSysIdAccess.put(resultSet.getString("username"), jsonDetail);

                jsonNeoClientIdToAccess.put(resultSet.getString("client_id"), jsonDetail);
            }

            // Log loaded print large data no needed
//    		LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
//    				"jsonSysIdAccess: " + jsonSysIdAccess.toString(), null);
            LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
                    "jsonNeoSysIdAccess and jsonNeoClientIdToAccess are initiated and ready.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public JSONObject isValidSysId(String systemId, String password, String ipAddress) {
        LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
                "TO VALIDATE: systemId: " + systemId + ", password: " + password + ", ipAddress: " + ipAddress + " -> JSONSYSIDACCESS: " + jsonSysIdAccess.toString(), null);

        JSONObject jsonReturn = new JSONObject();

        if (jsonSysIdAccess.has(systemId.trim())) {
            // systemId valid, check password
            JSONObject jsonDetail = jsonSysIdAccess.getJSONObject(systemId.trim());

            if (password.equals(jsonDetail.get("password"))) {
                // Password valid, check IPAddress
                String theRegisteredIPAddress = jsonDetail.getString("ipAddress");

                if (!theRegisteredIPAddress.trim().contains("ALL")) {
                    // Registered certain IP, split them by comma
//					String[] splittedIpAddress = theRegisteredIPAddress.split(",");
//					
//					for(int oh = 0; oh < splittedIpAddress.length; oh++){
//						if(ipAddress.trim().equals(splittedIpAddress[oh].trim())){
//							jsonReturn.put("status", 0); // Success
//							jsonReturn.put("description", "Validation success.");
//							
//							break;
//						} else {
//							jsonReturn.put("status", -3);
//							jsonReturn.put("description", "Unregistered IP Address.");							
//						}
//					}

                    if (theRegisteredIPAddress.contains(ipAddress.trim())) {
                        jsonReturn.put("status", 0); // Success
                        jsonReturn.put("description", "Validation success.");
                    } else {
                        jsonReturn.put("status", -3);
                        jsonReturn.put("description", "Unregistered IP Address.");
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
            jsonReturn.put("description", "Bad system id.");
        }

        return jsonReturn;
    }

    public String getClientId(String sysId) {
        String clientId = "";

        if (jsonSysIdAccess.has(sysId)) {
            JSONObject jsonData = jsonSysIdAccess.getJSONObject(sysId);

            clientId = jsonData.getString("clientId");
        }

        return clientId;
    }

    public String getNeoClientId(String sysId) {
        String clientId = "";

        if (jsonNeoSysIdAccess.has(sysId)) {
            JSONObject jsonData = jsonNeoSysIdAccess.getJSONObject(sysId);

            clientId = jsonData.getString("clientId");
        }

        return clientId;
    }
}
