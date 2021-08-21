package com.blastme.messaging.dummy;

import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class RabbitMQPumper {
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbitPublish;
    private Channel channelRabbitPublish;
    private Tool tool;
	
	public RabbitMQPumper() {
		tool = new Tool();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbitPublish = rabbitMqPooler.getConnection();
		
		channelRabbitPublish = rabbitMqPooler.getChannel(connectionRabbitPublish);
	}

	private void pumpToQueue(String queueName, String message) {
		try{
			channelRabbitPublish.queueDeclare(queueName, true, false, false, null);
			channelRabbitPublish.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.toString().getBytes());
			
			System.out.println("Publishing to queue " + queueName + ", message: " + message);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		if (args.length == 2) {
			String queueName = args[0];
			int queueCount = Integer.parseInt(args[1]);
			Tool tool = new Tool();
			
			RabbitMQPumper pumper = new RabbitMQPumper();
			
			for (int x = 0; x < queueCount; x++) {
				//String message = x + ". MESSAGE TO QUEUE";
				
				String messageId = tool.generateUniqueID();
				String msisdn = tool.generateUniqueId("NUMERIC", 8);
				String message = "{\"messageId\": \"" + messageId + "\", \"msisdn\": \"62811" + 
				msisdn + "\", \"message\": \"Chandra coba kirim SMS ke " + msisdn + "\", \"vendorSenderId\": \"MITSUBISHI\","
				+ "\"telecomId\": \"62001\", \"vendorParameter\": \"\", \"routedVendorId\": \"DMY190326\"}";
				
				pumper.pumpToQueue(queueName, message);
			}
			
			System.exit(0);
		} else {
			System.out.println("NOT CORRECT PARAMETER.");
			System.exit(-1);
		}

	}

}
