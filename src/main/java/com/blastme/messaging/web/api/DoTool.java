package com.blastme.messaging.web.api;

import java.io.File;
import java.util.HashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

public class DoTool implements Processor {
	private static Logger logger;
	//private static String SMSQueueName = "AUTOGENAUPLOADER_TOPUSH_BATCHID";
	//private static String SMSQueueName = "FIXSIDUPLOADER_TOPUSH_BATCHID";
	
	private Tool tool;
	
	private RabbitMQPooler rabbitMqPooler;
	private Connection connectionRabbit;
	private Channel channelRabbit;

	public DoTool() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("WEBAPI");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbit = rabbitMqPooler.getConnection();
		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
		
		LoggingPooler.doLog(logger, "INFO", "DoTool", "DoTool", false, false, false, "", 
				"Module DoTool is loaded.", null);			
	}

	private HashMap<String, String> pushToQueueSMSIncoming(String messageId, String queueName, String message){
		HashMap<String, String> mapResult = new HashMap<String, String>();
		
		try{
			channelRabbit.queueDeclare(queueName, true, false, false, null);
			channelRabbit.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.toString().getBytes());
			LoggingPooler.doLog(logger, "INFO", "DoTool", "pushToQueueSMSIncoming", false, false, false, messageId, 
					"Sending message " + message + " to message router queue " + queueName, null);	
			
			mapResult.put("status", "000");
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", "DoTool", "pushToQueueSMSIncoming", true, false, false, messageId, 
					"Failed sending message " + message + " to message router queue " + queueName, null);
			
			mapResult.put("status", "900");
		}
		
		return mapResult;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		@SuppressWarnings("unchecked")
		HashMap<String, String> mapIn = exchange.getIn().getBody(HashMap.class);
		LoggingPooler.doLog(logger, "INFO", "DoClient", "process", false, false, false, "", 
				"mapIn: " + tool.convertHashMapToJSON(mapIn).toString(), null);			

		try{
			if(mapIn.get("instruction").trim().equals("pushtoqueuesmsincoming")){
				String messageId = mapIn.get("messageid").trim();
				String queueName = mapIn.get("queuename").trim();
				String message = mapIn.get("queuemessage").trim();
				
				mapIn.putAll(pushToQueueSMSIncoming(messageId, queueName, message));
			}
		} catch (Exception e) {
			e.printStackTrace();
			mapIn.put("status", "900");
		}
		
		exchange.getOut().setBody(mapIn);
	}

}
