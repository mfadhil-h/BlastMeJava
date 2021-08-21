package com.blastme.messaging.toolpooler;

import java.io.File;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;
import com.blastme.messaging.configuration.Configuration;

public class ClientBalancePooler {
	private static Logger logger;
	
	private BasicDataSource bds = null;
	private Connection connection = null;
	private PreparedStatement pstmtGetBalance = null;
	private CallableStatement cstmtDeduction = null;
	
    private DateTimeFormatter formatter;

	public ClientBalancePooler() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("POOLER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate formatter
		formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

		// Initiate connection to Postgresql
        try {
            bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            
            //pstmtGetBalance = connection.prepareStatement("select now_balance from client_balance where client_id = ?");
            
			cstmtDeduction = connection.prepareCall("{ ? = call dofinancialaction(?, ?, ?, ?, ?, ?, ?, ?) }");
            
    		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "ClientBalancePooler", false, false, false, "", 
    				"Database connection is load and initiated.", null);
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "ClientBalancePooler", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
        }

		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "ClientBalancePooler", false, false, false, "", 
				"Module ClientBalancePooler is initiated and ready to serve.", null);				
	}

	public double getClientBalance(String clientId){
		double balance = 0.00;
		Connection getBalConnection = null;
		Statement getBalStatement = null;
	    ResultSet resultSet = null;

		try{
			//pstmtGetBalance.setString(1, clientId);
			getBalConnection = bds.getConnection();
			String query = "select now_balance from client_balance where client_id = '" + clientId.trim() + "'";
			getBalStatement = getBalConnection.createStatement();
			
            resultSet = getBalStatement.executeQuery(query);

            if(resultSet.next() == false){
            	// Record is not found - no balance
            	balance = 0.00000;
            } else {
            	// Record is found - use first one
            	balance = resultSet.getDouble("now_balance");
    			LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "getClientBalance", false, false, false, "", 
    					"clientId: " + clientId + " -> balance: " + String.format("%.5f", balance), null);				            	
            }            
		} catch (Exception e) {
			e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "getClientBalance", true, false, false, "", 
    				"Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
		} finally {
			 try {
	                if (resultSet != null)
	                    resultSet.close();
	                if (getBalStatement != null)
	                	getBalStatement.close();
	                if (getBalConnection != null)
	                	getBalConnection.close();
			 } catch(Exception e) {
					e.printStackTrace();
		    		LoggingPooler.doLog(logger, "DEBUG", "ClientBalancePooler", "getClientBalance", true, false, false, "", 
		    				"Failed to close query statement.", e);
			 }
		}
		
		return balance;
	}
	
	public JSONObject deductClientBalance(String messageId, String clientId, String usageType, double usageValue, String usageBy, 
			String usageDescription, String businessModel){
		JSONObject jsonDeduction = new JSONObject();
		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "deductClientBalance", false, false, false, "", 
				"messageId: " + messageId + ", clientId: " + clientId + ", usageValue: " + usageValue + ", BDValue: " + BigDecimal.valueOf(usageValue), null);				            	

		LocalDateTime now = LocalDateTime.now();
		try{			
			cstmtDeduction.registerOutParameter(1, Types.VARCHAR);
			cstmtDeduction.setString(2, messageId);
			cstmtDeduction.setString(3, usageType);
			cstmtDeduction.setString(4, usageBy);
			cstmtDeduction.setString(5, clientId);
			cstmtDeduction.setObject(6, now);
			cstmtDeduction.setString(7, usageDescription);
			cstmtDeduction.setBigDecimal(8, BigDecimal.valueOf(-1 * usageValue));
			cstmtDeduction.setString(9, businessModel);

			System.out.println("cstmtDeduction: " + cstmtDeduction.toString());
			cstmtDeduction.execute();
			
			String callResult = cstmtDeduction.getString(1);
			LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "deductClientBalance", false, false, false, "", 
					"Deduction callResult: " + callResult + ", cstmtDeduction query: " + cstmtDeduction.toString(), null);				            	
			
			if(Integer.parseInt(callResult) == 1){
				// Success
				jsonDeduction.put("status", 0);
			} else {
				// Failed
				jsonDeduction.put("status", -2); // Not enough or failed to deduct
			}
			
			jsonDeduction.put("deductionDateTime", now.format(formatter));
		} catch (Exception e) {
			jsonDeduction.put("status", -1);
			jsonDeduction.put("deductionDateTime", now.format(formatter));

			e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "deductClientBalance", true, false, false, "", 
    				"Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
		} 	

		LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "deductClientBalance", false, false, false, "", 
				"ClientBalancePooler jsonDeduction: " + jsonDeduction.toString(), null);				

		return jsonDeduction;
	}
}
