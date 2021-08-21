package com.blastme.messaging.smsbulk.transceiver;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TransceiverTelesindoLongNumber {
	private static Logger logger;	
	private static Tool tool;
	
	private RabbitMQPooler rabbitMqPooler;
	private com.rabbitmq.client.Connection connectionRabbit;
	private Channel channelRabbit;
	
	private String urlTelesindo = "http://122.102.41.66:8081";
	private String userTelesindo = "MODNT";
	private String passTelesindo = "w0pj931p";

	public TransceiverTelesindoLongNumber() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TRANSCEIVER_TELESINDO");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate Tool
		tool = new Tool();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbit = rabbitMqPooler.getConnection();
		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);

		LoggingPooler.doLog(logger, "INFO", "TransceiverTelesindo", "TransceiverTelesindo", false, false, false, "", 
				"Starting Transceiver TransceiverTelesindo ...", null);	
	}
	
	private void processQueueData(String message) {
		try {
			// Extract message
			JSONObject jsonMessage = new JSONObject(message);
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelesindo", "processQueueData", false, false, false, "", 
					"jsonMessage: " + jsonMessage.toString(), null);
			
			String messageId = jsonMessage.getString("messageId");
			String msisdn = jsonMessage.getString("msisdn");
			String theSMS = jsonMessage.getString("message");
//			String vendorSenderId = jsonMessage.getString("vendorSenderId");
//			String telecomId = jsonMessage.getString("telecomId");
//			String vendorParameter = jsonMessage.getString("vendorParameter");
//			String routedVendorId = jsonMessage.getString("routedVendorId");
//			String apiUserName = jsonMessage.getString("apiUserName");
			
			// Change prefix 62 to 0
			msisdn = msisdn.replace("62", "0");
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelesindo", "processQueueData", false, false, false, messageId, 
					"cleaned message - msisdn: " + msisdn + ", theSMS: " + theSMS, null);

			// Prepare the GET parameter to hit to telesindo
			String getParameter = "User=" + userTelesindo + "&Password=" + passTelesindo + "&PhoneNumber=" + URLEncoder.encode(msisdn, StandardCharsets.UTF_8.toString()) + "&Text=" + URLEncoder.encode(theSMS, StandardCharsets.UTF_8.toString());
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelesindo", "processQueueData", false, false, false, messageId, 
					"GET Parameter: " + getParameter, null);
			
			// Hit telesindo
			HashMap<String, String> mapHit = tool.hitHTTPGetGeneral(messageId, urlTelesindo, getParameter, null);
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelesindo", "processQueueData", false, false, false, messageId, 
					"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
			
			
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelesindo", "processQueueData", true, false, false, "", 
					"Error occured while hitting Telesindo.", e);
			e.printStackTrace();
		}
		
		
	}
	
	public void readQueue(String queueName) {
		try{
			channelRabbit.queueDeclare(queueName, true, false, false, null);
			channelRabbit.basicQos(50);
			
			LoggingPooler.doLog(logger, "INFO", "TransceiverTelesindo", "readQueue", false, false, true, "", 
					"Reading queue " + queueName + " is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelesindo", "readQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueData(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelesindo", "readQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbit.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbit.basicConsume(queueName, autoAck, consumer);
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile", "readQueue - " + queueName, true, false, false, "", 
					"Failed to access queue " + queueName, e);
		}
	}

	public static void main(String[] args) {
		String queueTelesindo = args[0];
		
		TransceiverTelesindoLongNumber lntrc = new TransceiverTelesindoLongNumber();
		lntrc.readQueue(queueTelesindo);
	}

}
