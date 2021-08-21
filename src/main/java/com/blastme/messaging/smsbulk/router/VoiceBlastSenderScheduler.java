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
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.Channel;

import io.lettuce.core.api.sync.RedisCommands;

public class VoiceBlastSenderScheduler {
	private static Logger logger;	
	private Tool tool;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private Connection connection = null;
	private PreparedStatement statementGetMesssageToSend = null;
	private PreparedStatement statementReadDetail = null;
	private PreparedStatement statementUpdateDetail = null;
    
    private SMSTransactionOperationPooler smsTransactionOperationPooler;
    private ClientPropertyPooler clientPropertyPooler;
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbitConsume;
    private Channel channelRabbitPublish;
    
	public VoiceBlastSenderScheduler() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("VOICEBLAST_SENDER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
	
		// Initiate smsTransactionOperationPooler
		smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		
		// Initiate TelecomPrefixPooler
		new TelecomPrefixPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
		
		// clientPropertyPooler
		clientPropertyPooler = new ClientPropertyPooler();
		
        try {
    		// Initiate RedisPooler
    		redisPooler = new RedisPooler();
    		redisCommand = redisPooler.redisInitiateConnection();
    		
    		// Initiate RabbitMQPooler
    		rabbitMqPooler = new RabbitMQPooler();	
    		connectionRabbit = rabbitMqPooler.getConnection();
    		
    		// Channel rabbit for consuming from VOICE_BLAST_SEND_BATCHID
    		channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Channel rabbit for publishing to SMS_INCOMING
    		channelRabbitPublish = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "VoiceBlastSender", false, false, false, "", 
    				"Application VoiceBlastProcessor is loaded and run.", null);
    		
    		// Select message to send
    		statementGetMesssageToSend = connection
    				.prepareStatement("select dtl.message_id, dtl.batch_id, dtl.msisdn, dtl.message, mmn.client_sender_id_id, csd.sender_id, "
    						+ "mmn.sending_schedule, dtl.processed_username, cuw.client_id from voice_blast_upload_detail as dtl "
    						+ "left join voice_blast_uploader as mmn on dtl.batch_id = mmn.batch_id "
    						+ "left join user_web as cuw on dtl.processed_username = cuw.username "
    						+ "left join client_senderid_sms as csd on mmn.client_sender_id_id = csd.client_sender_id_id "
    						+ "where (dtl.is_sent IS NULL) and (dtl.sent_status IS NULL) "
    						+ "and (mmn.sending_schedule IS NULL or mmn.sending_schedule < now()::timestamp) "
    						+ "and (dtl.processed_datetime >= NOW() - interval '1 day') and is_review_approved = true");
    		
    		// Select from voice blast detail
			statementReadDetail = connection
					.prepareStatement("select vdet.msisdn, vdet.message, vup.client_sender_id_id, csi.sender_id from "
							+ "voice_blast_upload_detail as vdet left join voice_blast_uploader as vup on "
							+ "vdet.batch_id = vup.batch_id left join client_senderid_sms as csi on "
							+ "vup.client_sender_id_id = csi.client_sender_id_id "
							+ "where (vdet.is_sent IS NULL or vdet.is_sent = false) and vdet.batch_id = ?");

    		// Get autogen_a_number as batchId
			statementUpdateDetail = connection
					.prepareStatement("update voice_blast_upload_detail set is_sent = true, sent_datetime = ?, "
							+ "sent_status = ?, route_key = ? where batch_id = ? and msisdn = ?");
			
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "VoiceBlastSender", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
    		
    		System.exit(-1);
        }		
	}

	public void runTheSendingJob() {
		try {
			System.out.println("Running schedule for sending voice message job.");
			
			JobDetail job1 = JobBuilder.newJob(VoiceBlastSenderNew.class)
                    .withIdentity("job1", "group1").build();
			
			job1.getJobDataMap().put("logger", logger);
			job1.getJobDataMap().put("tool", tool);
			job1.getJobDataMap().put("redisPooler", redisPooler);
			job1.getJobDataMap().put("redisCommand", redisCommand);
			job1.getJobDataMap().put("jdbcConnection", connection);
			job1.getJobDataMap().put("statementGetMesssageToSend", statementGetMesssageToSend);
			job1.getJobDataMap().put("statementReadDetail", statementReadDetail);
			job1.getJobDataMap().put("statementUpdateDetail", statementUpdateDetail);
			job1.getJobDataMap().put("smsTransactionOperationPooler", smsTransactionOperationPooler);
			job1.getJobDataMap().put("clientPropertyPooler", clientPropertyPooler);
			job1.getJobDataMap().put("rabbitMqPooler", rabbitMqPooler);
			job1.getJobDataMap().put("rabbitConnection", connectionRabbit);
			job1.getJobDataMap().put("channelRabbitConsume", channelRabbitConsume);
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
		VoiceBlastSenderScheduler scheduler = new VoiceBlastSenderScheduler();
		scheduler.runTheSendingJob();
	}

}
