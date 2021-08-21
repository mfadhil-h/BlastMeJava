package com.blastme.messaging.smsbulk.router;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.TimeZone;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.Channel;

public class SMSFixANumberSenderIdScheduler {
	private static Logger logger;	
	private Tool tool;
	
	private Connection connection = null;
	//private Prepared,Statement statementUnproccessedFixSenderSMS = null;
	private PreparedStatement statementDeleteProcessedScheduled = null;
	private PreparedStatement statementSelectScheduled = null;
	private PreparedStatement statementUpdateTransaction = null;

	private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbitPublish;
    
	public SMSFixANumberSenderIdScheduler() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("FIXANUMBER_PROCESSOR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
		
        try {
    		// Initiate RabbitMQPooler
    		rabbitMqPooler = new RabbitMQPooler();	
    		connectionRabbit = rabbitMqPooler.getConnection();
    		
    		// Channel rabbit for publishing to SMS_INCOMING
    		channelRabbitPublish = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "VoiceBlastSender", false, false, false, "", 
    				"Application VoiceBlastProcessor is loaded and run.", null);
    		
    		// Select from fix_a_number_sid
//			statementUnproccessedFixSenderSMS = connection
//					.prepareStatement("select fas.batch_id, fas.username, cc.client_id, fas.sending_schedule from fix_a_number_sid as fas "
//							+ "left join user_api as cc on fas.username = cc.username where is_processed = false");
    		
    		// select from fix_a_number_scheduled_message
    		statementSelectScheduled = connection
    				.prepareStatement("SELECT message_id, msisdn, message, client_id, telecom_id, client_sender_id, sms_channel, api_username, message_length, message_count, client_price_per_submit, schedule " + 
    						"FROM fix_a_number_scheduled_message");
    		
    		// delete from fix_a_number_scheduled_message 
    		statementDeleteProcessedScheduled = connection
    				.prepareStatement("delete from fix_a_number_scheduled_message where message_id = ?");
    		
    		// update transaction
    		statementUpdateTransaction = connection
    				.prepareStatement("update transaction_sms set status_code = '003' where message_id = ?");
    		
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "VoiceBlastSender", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
    		
    		System.exit(-1);
        }				
	}
	
	public void runTheProcessorJob() {
		try {
			System.out.println("Running schedule for checking pending message job.");
			
			JobDetail job1 = JobBuilder.newJob(SMSFixANumberSenderIdScheduleProcessor.class)
                    .withIdentity("job1", "group1").build();
			
			job1.getJobDataMap().put("logger", logger);
			job1.getJobDataMap().put("tool", tool);
			job1.getJobDataMap().put("jdbcConnection", connection);
			//job1.getJobDataMap().put("statementUnproccessedFixSenderSMS", statementUnproccessedFixSenderSMS);
			job1.getJobDataMap().put("statementSelectScheduled", statementSelectScheduled);
			job1.getJobDataMap().put("statementDeleteProcessedScheduled", statementDeleteProcessedScheduled);
			job1.getJobDataMap().put("statementUpdateTransaction", statementUpdateTransaction);
			job1.getJobDataMap().put("rabbitMqPooler", rabbitMqPooler);
			job1.getJobDataMap().put("rabbitConnection", connectionRabbit);
			job1.getJobDataMap().put("channelRabbitPublish", channelRabbitPublish);
 
            Trigger trigger1 = TriggerBuilder.newTrigger()
                    .withIdentity("cronTrigger1", "group1")
                    .forJob(job1)
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/30 * * * * ?"))
                    .build();
             
            Scheduler scheduler1 = new StdSchedulerFactory().getScheduler();
            scheduler1.start();
            scheduler1.scheduleJob(job1, trigger1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SMSFixANumberSenderIdScheduler scheduler = new SMSFixANumberSenderIdScheduler();
		scheduler.runTheProcessorJob();
	}

}
