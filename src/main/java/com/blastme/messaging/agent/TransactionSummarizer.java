package com.blastme.messaging.agent;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;

public class TransactionSummarizer {
	private static Logger logger;	

	private Connection connection = null;
	private PreparedStatement statement = null;
    private ResultSet resultSet = null;
    
    private PreparedStatement statementGenerated = null;
    
    DateTimeFormatter formatterSQL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	public TransactionSummarizer() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("CSV_TRANSACTION_GENERATOR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate connection to Postgresql
		try{
	        BasicDataSource bds = DataSource.getInstance().getBds();
	        connection = bds.getConnection();
			
	        statement = connection.prepareStatement("select request_id, username, start_datetime, end_datetime, client_id, search_keyword, search_parameter "
	        		+ "from report_request_transaction_sms where is_generated = false");
	        
	        statementGenerated = connection.prepareStatement("update report_request_transaction_sms set is_generated = true, file_path = ? where request_id = ?");

	        LoggingPooler.doLog(logger, "INFO", "CSVTransactionGenerator", "CSVTransactionGenerator", false, false, false, "", 
					"Starting CSVTransactionGenerator app ...", null);							
		} catch (Exception e) {			
			LoggingPooler.doLog(logger, "INFO", "CSVTransactionGenerator", "CSVTransactionGenerator", true, false, false, "", 
					"Failed to run CSVTransactionGenerator. Error occured.", e);	
			
			e.printStackTrace();
			
			System.exit(-1);
		}		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
