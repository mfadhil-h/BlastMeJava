package com.blastme.messaging.dummy;

import java.util.Date;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzCoba {
	
	private void goRun() {
		try {
			System.out.println("Is going run!!");
			
			JobDetail job1 = JobBuilder.newJob(cobaQuartzJob.class)
                    .withIdentity("job1", "group1").build();
 
            Trigger trigger1 = TriggerBuilder.newTrigger()
                    .withIdentity("cronTrigger1", "group1")
                    .forJob(job1)
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
                    .build();
             
            Scheduler scheduler1 = new StdSchedulerFactory().getScheduler();
            scheduler1.start();
            scheduler1.scheduleJob(job1, trigger1);
			
            int count = 0;
            while(count < 50) {
            	System.out.println("OH OH OH");
            	count = count + 1;
            	Thread.sleep(1000);
            }
            
            scheduler1.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		QuartzCoba coba = new QuartzCoba();
		coba.goRun();
	}
}
