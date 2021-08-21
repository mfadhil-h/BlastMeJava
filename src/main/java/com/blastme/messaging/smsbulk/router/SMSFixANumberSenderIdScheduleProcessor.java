package com.blastme.messaging.smsbulk.router;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Date;

import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.blastme.messaging.toolpooler.LoggingPooler;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class SMSFixANumberSenderIdScheduleProcessor implements Job{
	private String queueProccessing = "SMS_INCOMING";	
    //private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
    private static Logger logger;		
	//private PreparedStatement statementUnproccessedFixSenderSMS = null;
	private PreparedStatement statementDeleteProcessedScheduled = null;
	private PreparedStatement statementSelectScheduled = null;
	private PreparedStatement statementUpdateTransaction = null;
	
    private Channel channelRabbitPublish;

	public SMSFixANumberSenderIdScheduleProcessor() {
		// Alert of initing
		System.out.println("INTIATING FIX SENDER ID PROCESSOR!!!");
	}
	
	private void doProcess() {
		try {
			System.out.println("Do processing ...");
			
			// Do sending - get messages to send
			ResultSet resultSet = statementSelectScheduled.executeQuery();
									
			while (resultSet.next()) {
				// Read the data
				String messageId = resultSet.getString("message_id");
				String msisdn = resultSet.getString("msisdn");
				String message = resultSet.getString("message");
				String clientId = resultSet.getString("client_id");
				String telecomId = resultSet.getString("telecom_id");
				String clientSenderId = resultSet.getString("client_sender_id");
				String smsChannel = resultSet.getString("sms_channel");
				String apiUsername = resultSet.getString("api_username");
				int messageLength = resultSet.getInt("message_length");
				int messageCount = resultSet.getInt("message_count");
				double clientPricePerSubmit = resultSet.getDouble("client_price_per_submit");
				LocalDateTime sendingSchedule = resultSet.getObject("schedule", LocalDateTime.class);
				
				// Check the schedule
				if (sendingSchedule.isBefore(LocalDateTime.now())) {
					// It is the schedule, send NOW?
					// Delete the data with mesageId 
					statementDeleteProcessedScheduled.setString(1, messageId);
					
					int rowImpact = statementDeleteProcessedScheduled.executeUpdate();
					
					System.out.println("Delete rowImpact: " + rowImpact);
					
					if (rowImpact > 0) {
						// Delete success, baru diproses untuk memastikan tidak ada 1 row yang dikirim sms berkali-kali
						// Update transaction status to 003
						statementUpdateTransaction.setString(1, messageId);
						
						statementUpdateTransaction.executeUpdate();										
						
						// Send to queue INCOMING_SMS
						JSONObject jsonIncoming = new JSONObject();

						jsonIncoming.put("messageId", messageId);
						jsonIncoming.put("apiUserName", apiUsername);
						jsonIncoming.put("msisdn", msisdn);
						jsonIncoming.put("message", message);
						jsonIncoming.put("clientId", clientId);
						jsonIncoming.put("telecomId", telecomId);
						jsonIncoming.put("clientSenderId", clientSenderId);
						jsonIncoming.put("smsChannel",smsChannel);
						jsonIncoming.put("messageLength", messageLength);
						jsonIncoming.put("messageCount", messageCount);
						jsonIncoming.put("clientPricePerSubmit", String.format("%.5f", clientPricePerSubmit));

						// Publish jsonIncoming to queue INCOMING_SMS
						channelRabbitPublish.queueDeclare(queueProccessing, true, false, false, null);
						channelRabbitPublish.basicPublish("", queueProccessing, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
						LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "doProcess", false, false, false, messageId, 
								"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);					
					} else {
						// Delete gagal, tidak diproses
						LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "doProcess", false, false, false, messageId, 
								"Failed to delete table Fix Number Scheduled. Not sending to ROUTER!", null);					
					}	
				}				
			}
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "SMSFixANumberSenderIdSchedulerProcesor", "doProcessing", true, false, false, "", 
					"Failed to process the message. Error occured", e);								
		}
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		System.out.println(new Date() + ". Sending message job!");
		
		JobDataMap data = context.getJobDetail().getJobDataMap();
		
		logger = (Logger) data.get("logger");

		//connection = (Connection) data.get("jdbcConnection");
		//statementUnproccessedFixSenderSMS = (PreparedStatement) data.get("statementUnproccessedFixSenderSMS");		
		statementSelectScheduled = (PreparedStatement) data.get("statementSelectScheduled");
		statementDeleteProcessedScheduled = (PreparedStatement) data.get("statementDeleteProcessedScheduled");
		statementUpdateTransaction = (PreparedStatement) data.get("statementUpdateTransaction");
		
		channelRabbitPublish = (Channel) data.get("channelRabbitPublish");
		
		// Sending
		doProcess();		
	}

}
