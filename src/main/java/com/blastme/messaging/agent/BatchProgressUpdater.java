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

public class BatchProgressUpdater {
	private static Logger logger;	

	private Connection connection = null;
	private PreparedStatement statementBatchRead = null;
    private ResultSet resultSetBatchRead = null;
    
    private PreparedStatement statementTransaction = null;
    private ResultSet resultSetTransaction = null;
    
    private PreparedStatement statementBatchUpdate = null;
    
    DateTimeFormatter formatterSQL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	public BatchProgressUpdater() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("BATCH_PROGRESS UPDATER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate connection to Postgresql
		try{
	        BasicDataSource bds = DataSource.getInstance().getBds();
	        connection = bds.getConnection();
			
	        statementBatchRead = connection.prepareStatement("select batch_id from autogenbnumberprogress where sent_count < sms_count");
	        
	        statementTransaction = connection.prepareStatement("select sum(sms_count) as theCount from transaction_sms where batch_id = ? and status_code != '003'");
	        
	        statementBatchUpdate = connection.prepareStatement("update autogenbnumberprogress set sent_count = ? where batch_id = ?");

	        LoggingPooler.doLog(logger, "INFO", "BatchProgressUpdater", "BatchProgressUpdater", false, false, false, "", 
					"Starting CSVTransactionGenerator app ...", null);							
		} catch (Exception e) {			
			LoggingPooler.doLog(logger, "INFO", "BatchProgressUpdater", "BatchProgressUpdater", true, false, false, "", 
					"Failed to run BatchProgressUpdater. Error occured.", e);	
			
			e.printStackTrace();
			
			System.exit(-1);
		}
	}
	
	private void doUpdateProgress(){
		try{
			System.out.println("Checking ...");

			resultSetBatchRead = statementBatchRead.executeQuery();
			
			// Read batch yang belum 100%
			while(resultSetBatchRead.next()){
				String batchId = resultSetBatchRead.getString("batch_id");
				
				// Read transaction dengan no batch ini
				statementTransaction.setString(1, batchId);
				
				resultSetTransaction = statementTransaction.executeQuery();
				
				while(resultSetTransaction.next()){
					// Update batch progress dengan theCount
					int theCount = 0;
					if(resultSetTransaction.getObject("theCount") != null){
						theCount = resultSetTransaction.getInt("theCount");
					}
					
					statementBatchUpdate.setInt(1, theCount);
					statementBatchUpdate.setString(2, batchId);
					
					statementBatchUpdate.executeUpdate();
					
					System.out.println("Updating batchId: " + batchId + " to count: " + theCount);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void doDaemon(){
		try{
			while(true){
				doUpdateProgress();
				
				Thread.sleep(5*60*1000); // per 5 minutes
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		BatchProgressUpdater updater = new BatchProgressUpdater();
		updater.doDaemon();
	}

}
