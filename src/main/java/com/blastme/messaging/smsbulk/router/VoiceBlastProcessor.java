package com.blastme.messaging.smsbulk.router;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.TimeZone;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class VoiceBlastProcessor {
	private static Logger logger;	
	private Tool tool;
	
	private static String queueBatchToProcess = "VOICE_BLAST_TOPUSH_BATCHID";
//	private static String queueIncomingMessage = "SMS_INCOMING";
//	private static String smsChannel = "VOICEBLAST_WEBUPLOADER";
	private String batchFileDirectory;
//	private DateTimeFormatter dtfNormal = DateTimeFormatter.ofPattern("yy-MM-dd HH:mm:ss.SSS");
	
//	private static RedisPooler redisPooler;
//	private static RedisCommands<String, String> redisCommand;

	private Connection connection = null;
	private PreparedStatement statement = null;
	private PreparedStatement statementIsProcessed = null;
	private PreparedStatement statementVoiceDetail = null;
    private ResultSet resultSet = null;
    
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbit;
	
	public VoiceBlastProcessor() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("VOICEBLAST_PROCESSOR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
		
		// Initiate batchFileDirectory
		batchFileDirectory = Configuration.getVoiceBlastBNumberDirectory();
		
		
		// Initiate smsTransactionOperationPooler
		//smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		
		// Initiate TelecomPrefixPooler
		new TelecomPrefixPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
		
		// clientPropertyPooler
		//clientPropertyPooler = new ClientPropertyPooler();
		
        try {
    		// Initiate RedisPooler
//    		redisPooler = new RedisPooler();
//    		redisCommand = redisPooler.redisInitiateConnection();
    		
    		// Initiate RabbitMQPooler
    		rabbitMqPooler = new RabbitMQPooler();	
    		connectionRabbit = rabbitMqPooler.getConnection();
    		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "VoiceBlastProcessor", false, false, false, "", 
    				"Application VoiceBlastProcessor is loaded and run.", null);
    		
    		// Select job
			statement = connection.prepareStatement("select batch_id, user_id, message, file_path, trx_datetime, is_processed from voice_blast_uploader where batch_id = ?");

    		// Get autogen_a_number as batchId
			statementIsProcessed = connection
					.prepareStatement("update voice_blast_uploader set is_processed = true where batch_id = ?");
			
			// Insert into voice detail
			statementVoiceDetail = connection
					.prepareStatement("INSERT INTO voice_blast_upload_detail(message_id, batch_id, msisdn, message, "
							+ "processed_datetime, processed_username) VALUES (?, ?, ?, ?, ?, ?)");

        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "VoiceBlastProcessor", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
    		
    		System.exit(-1);
        }

	}
	
	private void updateIsProcessed(String batchId){
		try{			
			statementIsProcessed.setString(1, batchId);
			statementIsProcessed.executeUpdate();
			
			LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastProcessor", "updateIsProcessed", false, false, false,"", 
					"Succesfully update batchId " + batchId + " ALREADY PROCESSED TO TRUE.", null);	
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastProcessor", "updateIsProcessed", true, false, false,"", 
					"Failed to update batchId " + batchId + " ALREADY PROCESSED TO TRUE.", e);				
		}
	}
	
	private void insertVoiceUploadDetail(String messageId, String batchId, String bNumber, String message, boolean isApproved, String userName) {
		try {
			
			statementVoiceDetail.setString(1, messageId);
			statementVoiceDetail.setString(2, batchId);
			statementVoiceDetail.setString(3, bNumber);
			statementVoiceDetail.setString(4, message);
			statementVoiceDetail.setObject(5, LocalDateTime.now());
			statementVoiceDetail.setString(6, userName);
			
			System.out.println("Query: " + statementVoiceDetail.toString());
			
			int x = statementVoiceDetail.executeUpdate();
			
			if (x > 0) {
				System.out.println("Successfully insert detail voice upload - batchId: " + batchId + ", bNumber: " + bNumber);
			} else {
				System.out.println("Failed to insert detail voice upload - batchId: " + batchId + ", bNumber: " + bNumber);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void doMergingProcess( String batchId, String fileName, String theMessageTemplate, String userName) {
		String fullFilePath = batchFileDirectory + fileName;
			
		try {
			// Open the file excel
			File file = new File(fullFilePath);
			
			LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastProcessor", "doMerghingProcess", false, false, false,"", 
					"Merging data batchId: " + batchId + ", file: " + fullFilePath + ", messageTemplate: " + theMessageTemplate, null);	

			FileInputStream inputStream = new FileInputStream(file);
			
			Workbook workbook = WorkbookFactory.create(file);

			Sheet sheet = workbook.getSheetAt(0);
			int rowCount = sheet.getLastRowNum() - sheet.getFirstRowNum();
			System.out.println("rowCount: " + rowCount);
			
			for (int x = 1; x <= rowCount; x++) {
				// Start from row 1, since row 0 is for HEADER/TITLE
				Row row = sheet.getRow(x);
				Row rowHeader = sheet.getRow(0);
				
				System.out.println("lastCellNum: " + row.getLastCellNum());

				// Get BNumber -> kolom 0
				CellType bType = row.getCell(0).getCellType();
				
				String bNumber = "";				
				if (bType == CellType.STRING) {
					bNumber = row.getCell(0).getStringCellValue();
				} else if (bType == CellType.NUMERIC) {
					bNumber = String.format("%.0f", row.getCell(0).getNumericCellValue());
				} else if (bType == CellType.BOOLEAN) {
					bNumber = Boolean.toString(row.getCell(0).getBooleanCellValue());
				} else if (bType == CellType.BLANK) {
					bNumber = "";
				}
				System.out.println(x + ". bNumber: " + bNumber);

				String messagePenampung = theMessageTemplate;
				for (int y = 1; y < row.getLastCellNum(); y++) {
					// Mulai dari kolom 1 karena kolom 0 adalah BNumber
					String cellName = rowHeader.getCell(y).getStringCellValue();
					
					CellType type = row.getCell(y).getCellType();
					
					String cellValue = "";
					
					if (type == CellType.STRING) {
						cellValue = row.getCell(y).getStringCellValue();
					} else if (type == CellType.NUMERIC) {
						cellValue = String.format("%.0f", row.getCell(y).getNumericCellValue());
					} else if (type == CellType.BOOLEAN) {
						cellValue = Boolean.toString(row.getCell(y).getBooleanCellValue());
					} else if (type == CellType.BLANK) {
						cellValue = "";
					}
					
					System.out.println("cellName: " + cellName + ", cellValue: " + cellValue);
					
					// Put into message template
					messagePenampung = messagePenampung.replace("[" + cellName + "]", cellValue);
				}		
				
				System.out.println(x + ". BNumber: " + bNumber + ", message: " + messagePenampung);
				
				// Assign messageId
	    		String messageId = tool.generateUniqueID();

				// Insert into table voice_blast_uploader_detail
				insertVoiceUploadDetail(messageId, batchId, bNumber, messagePenampung, false, userName);
			}
			
			workbook.close();
			inputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastProcessor", "doMerghingProcess", true, false, true,"", 
					"Failed merging data batchId: " + batchId + ", file: " + fullFilePath + ", messageTemplate: " + theMessageTemplate + ". ERROR OCCURED.", e);	
		}
	}
	
	private void processQueueBatch(String queueMessage){
		try{			
			// Get batchId and clientId from queueMessage
			JSONObject jsonMessage = new JSONObject(queueMessage);
			
			String batchId = jsonMessage.getString("batchId");
			String clientId = jsonMessage.getString("clientId");
			String username = jsonMessage.getString("username");
			
			LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastProcessor", "processQueueBatch", false, false, false,"", 
					"Incoming data - batchId: " + batchId + ", clientId: " + clientId + ", username: " + username, null);	

			// Query table autogenbnumberuploaderdetail, get the batch which alerady_processed == false or null
			try{
				statement.setString(1, batchId);
				
				resultSet = statement.executeQuery();
				
				while(resultSet.next()){	
	            	// Get now localdatetime
					String fileName = resultSet.getString("file_path");
					String theMessage = resultSet.getString("message");
					
					LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastProcessor", "processQueueBatch", false, false, false,"", 
							"batchId: " + batchId + ", fileName: " + fileName + ", theMessageTemplate: " + theMessage, null);	

					// Process the merging between excel file and the message
					doMergingProcess(batchId, fileName, theMessage, username);
					
					// Update status is_processed = true
					updateIsProcessed(batchId);
				}				
			} catch (Exception e) {
				e.printStackTrace();
	    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "processQueueBatch", true, false, false, "", 
	    				"Failed to process incoming instruction. Error occured.", e);
			} 
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void readQueue(){
		// Listen to queue that contains information about which batchnumber to process - queuename: AUTOGEN_BATCH_INSTRUCT
		// Incoming message will be: {"batchId": "1928394830292", "clientId": "1029384", "username": "chandrapawitra02"}
		try{
			channelRabbit.queueDeclare(queueBatchToProcess, true, false, false, null);			
			LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "readQueue", false, false, true, "", 
					"Reading queue is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "readQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueBatch(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "readQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbit.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbit.basicQos(1); // Read one by one, so if other app FixANumberProcessor running, it can share load
			channelRabbit.basicConsume(queueBatchToProcess, autoAck, consumer);			
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "VoiceBlastProcessor", "readQueue", true, false, false, "", 
					"Failed to access queue " + queueBatchToProcess, e);
		}
	}

	public static void main(String[] args) {
		// WHAT TO DO BY THIS MODULE:
		// READ QUEUE THAT CONTAINS INFO WHICH BATCH ID JUST UPLOADED
		
		System.out.println("Starting Voice Blast Processor ...");
		
		VoiceBlastProcessor processor = new VoiceBlastProcessor();
		processor.readQueue();
	}

}
