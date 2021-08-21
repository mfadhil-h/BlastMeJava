package com.blastme.messaging.agent;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.TimeZone;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;

public class CSVTransactionGenerator {
	private static Logger logger;	

	private Connection connection = null;
	private PreparedStatement statement = null;
    private ResultSet resultSet = null;
    
    private PreparedStatement statementGenerated = null;
    
    DateTimeFormatter formatterSQL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
	public CSVTransactionGenerator() {
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
	
	private void readTransaction(String requestId, String clientId, LocalDateTime startDateTime, LocalDateTime endDateTime, String searchKeyword, String searchParameter){
		String theQuery = "select a.message_id, a.transaction_date, a.msisdn, a.message, b.country_name, a.prefix, a.status_code, "
	        		+ "f.description, a.receiver_type, d.client_name, a.currency, a.message_encodng, a.message_length, a.sms_count, a.client_price_per_unit, a.client_price_total, "
	        		+ "a.client_sender_id, a.batch_id from transaction_sms as a left join country as b on a.country_code = b.country_id left join client as d on a.client_id = d.client_id "
	        		+ "left join transaction_status as f on a.status_code = f.status_code where a.transaction_date <= '" + endDateTime.format(formatterSQL) + 
	        		"' and a.transaction_date >= '" + startDateTime.format(formatterSQL) + "' ";
		
		if(clientId.trim().equals("ALL")){
			// Searching
			if(searchKeyword != null && searchKeyword.trim().length() > 0){
				if(searchParameter.trim().equals("messageId")){
					theQuery = theQuery + " and a.message_id = '" + searchKeyword.trim() + "'";
				} else if(searchParameter.trim().equals("batchId")){
					theQuery = theQuery + " and a.batch_id = '" + searchKeyword.trim() + "'";
				} else if(searchParameter.trim().equals("senderId")){
					theQuery = theQuery + " and a.client_sender_id = '" + searchKeyword.trim() + "'";
				} else if(searchParameter.trim().equals("destination")){
					theQuery = theQuery + " and a.msisdn = '" + searchKeyword.trim() + "'";
				}
			}
		} else {
			// Add clientId 
			theQuery = theQuery + " and a.client_id = '" + clientId + "'";
			
			// Searching
			if(searchKeyword != null && searchKeyword.trim().length() > 0){
				if(searchParameter.trim().equals("messageId")){
					theQuery = theQuery + " and a.message_id = '" + searchKeyword.trim() + "'";
				} else if(searchParameter.trim().equals("batchId")){
					theQuery = theQuery + " and a.batch_id = '" + searchKeyword.trim() + "'";
				} else if(searchParameter.trim().equals("senderId")){
					theQuery = theQuery + " and a.client_sender_id = '" + searchKeyword.trim() + "'";
				} else if(searchParameter.trim().equals("destination")){
					theQuery = theQuery + " and a.msisdn = '" + searchKeyword.trim() + "'";
				}
			}
		}
		
		System.out.println("theQuery: " + theQuery);
		
		// Prepared for CSV File
		String directoryPath = Configuration.getCsvTransactionReportPath();
		String fileName = requestId + ".csv";
		String fullPath = directoryPath + fileName;
		System.out.println(fullPath);
		
		BufferedWriter writer = null;
		CSVPrinter csvPrinter = null;
		
		try{
			writer = Files.newBufferedWriter(Paths.get(fullPath));

			csvPrinter = new CSVPrinter(writer, CSVFormat.EXCEL.withHeader("Date Time", "Batch ID", "Message ID", "Sender ID", "MSISDN", "Message", "Status", 
                    		"Client", "Country", "Encoding", "Message Length", "SMS Count", "Total Price"));

			// Query transaction table
			connection.setAutoCommit(false);
			Statement statementTrx = connection.createStatement();
			statementTrx.setFetchSize(1000); // Biar gak kehabisan memory. Baca tiap 100 rows

			ResultSet rsTrx = statementTrx.executeQuery(theQuery);
			
			while(rsTrx.next()){
				String trxDateTime = rsTrx.getObject("transaction_date", LocalDateTime.class).format(formatterSQL);
				String batchId = rsTrx.getString("batch_id");
				String messageId = rsTrx.getString("message_id");
				String senderId = rsTrx.getString("client_sender_id").replace("*AUTOGEN*-", "");
				String msisdn = rsTrx.getString("msisdn");
				String message = rsTrx.getString("message");
				String status = rsTrx.getString("description");
				String client = rsTrx.getString("client_name");
				String country = rsTrx.getString("country_name");
				String encoding = rsTrx.getString("message_encodng");
				String messageLength = Integer.toString(rsTrx.getInt("message_length"));
				String smsCount = Integer.toString(rsTrx.getInt("sms_count"));
				String clientPrice = rsTrx.getBigDecimal("client_price_total").toString();
				
				csvPrinter.printRecord(Arrays.asList(trxDateTime, batchId, messageId, senderId, msisdn, message, status, client, 
						country, encoding, messageLength, smsCount, clientPrice));
				
				System.out.println(requestId + ". Done processing " + messageId);				
			}
			
			// Update process is done
			statementGenerated.setString(1, fileName);
			statementGenerated.setString(2, requestId);
			
			statementGenerated.executeUpdate();

			statementTrx.close();
			
	        LoggingPooler.doLog(logger, "INFO", "CSVTransactionGenerator", "readTransaction", false, false, false, "", 
					"Generating report for requestId: " + requestId + " is DONE.", null);							
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				csvPrinter.flush();
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
	}
	
	private void generateTheCSV(){
		System.out.println("Do generate the CSV");
		try{
			// Read report request
			System.out.println("statement: " + statement.toString());
			resultSet = statement.executeQuery();
			
			while(resultSet.next()){
				String requestId = resultSet.getString("request_id");
				System.out.println("Processing requestId: " + requestId);
				
				LocalDateTime startDateTime = resultSet.getObject("start_datetime", LocalDateTime.class);
				LocalDateTime endDateTime = resultSet.getObject("end_datetime", LocalDateTime.class);
				String clientId = resultSet.getString("client_id");
				String searchKeyword = resultSet.getString("search_keyword");
				String searchParameter = resultSet.getString("search_parameter");
				
				readTransaction(requestId, clientId, startDateTime, endDateTime, searchKeyword, searchParameter);
			}
			
			System.out.println("Done generating all report request.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void runTheAgent(){
		while(true){
			try {
				generateTheCSV();
				
				// Delay 5 minutes
				Thread.sleep(5 * 60 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				
				LoggingPooler.doLog(logger, "INFO", "CSVTransactionGenerator", "runTheAgent", true, false, false, "", 
						"Failed to run CSVTransactionGenerator agent. Error occured.", e);	
			}
		}
	}

	public static void main(String[] args) {
		CSVTransactionGenerator generator = new CSVTransactionGenerator();
		generator.runTheAgent();
	}

}
