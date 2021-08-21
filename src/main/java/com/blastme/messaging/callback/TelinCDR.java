package com.blastme.messaging.callback;

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
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.Tool;

import io.lettuce.core.api.sync.RedisCommands;

public class TelinCDR {
	private static Logger logger;
	private Tool tool;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private Connection connection = null;
	private PreparedStatement pstmtGetFailedCall = null;

	public TelinCDR() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TELIN_CDR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
		
		try {
    		// Initiate RedisPooler
    		redisPooler = new RedisPooler();
    		redisCommand = redisPooler.redisInitiateConnection();
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "TelinCDR", "TelinCDR", false, false, false, "", 
    				"Application TelinCDR is loaded and run.", null);
    		
    		// Initiate pstmtGetFailedCall
//    		pstmtGetFailedCall = connection.prepareStatement("select message_id, transaction_date, api_username from transaction_sms "
//			+ "where status_code = '002' and message_encodng = 'VOICE' and (transaction_date >= NOW() - interval '1 day')");
//    		pstmtGetFailedCall = connection.prepareStatement("select message_id, transaction_date, api_username from transaction_sms "
//    				+ "where status_code = '101' and (transaction_date >= NOW() - interval '1 day')");
    		pstmtGetFailedCall = connection.prepareStatement("select trs.message_id, trs.transaction_date, trs.api_username, ccc.client_group_id from transaction_sms as trs\n" + 
    				"left join client as ccc on trs.client_id = ccc.client_id where trs.status_code = '002' and trs.message_encodng = 'VOICE' and (trs.transaction_date >= NOW() - interval '1 day')");
		} catch (Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "TelinCDR", "TelinCDR", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
    		
    		System.exit(-1);
		}
	}
	
	private void doGetCDR() {
		try {
			System.out.println("Running schedule for sending voice message job.");
			
			JobDetail job1 = JobBuilder.newJob(TelinCDRActor.class)
                    .withIdentity("job1", "group1").build();
			
			job1.getJobDataMap().put("logger", logger);
			job1.getJobDataMap().put("tool", tool);
			job1.getJobDataMap().put("redisPooler", redisPooler);
			job1.getJobDataMap().put("redisCommand", redisCommand);
			job1.getJobDataMap().put("jdbcConnection", connection);
			job1.getJobDataMap().put("pstmtGetFailedCall", pstmtGetFailedCall);
 
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
			
    		LoggingPooler.doLog(logger, "INFO", "TelinCDR", "doGetCDR", true, false, false, "", 
    				"Failed to run job for TelinCDRActor.", e);
		}
	}
	

	public static void main(String[] args) {
		TelinCDR cdr = new TelinCDR();
		cdr.doGetCDR();
	}

}
