package com.nayak.springbatch.config;

import lombok.RequiredArgsConstructor;
import org.quartz.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.Date;

@Configuration
@RequiredArgsConstructor
public class QuartzConfig {

    @Bean
    public JobDetail jobDetail() {
        return JobBuilder.newJob(BatchJobQuartzLauncher.class)
                .withIdentity("batchJob")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger jobTrigger() {
        SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
                .simpleSchedule()
                .withIntervalInHours(24)
                .repeatForever();

        return TriggerBuilder.newTrigger()
                .forJob(jobDetail())
                .withIdentity("dailyTrigger")
                .withSchedule(scheduleBuilder)
                .build();
    }

    @Bean
    public Scheduler scheduler(SchedulerFactoryBean factory) throws Exception {
        Scheduler scheduler = factory.getScheduler();
        scheduler.scheduleJob(jobDetail(), jobTrigger());
        return scheduler;
    }


    @RequiredArgsConstructor
    public static class BatchJobQuartzLauncher extends QuartzJobBean {

        private final JobLauncher jobLauncher;

        private final Job reportJob;

        @Override
        protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
            try {
                JobParameters params = new JobParametersBuilder()
                        .addDate("scheduledTime", new Date())
                        .toJobParameters();

                jobLauncher.run(reportJob, params);
            } catch (Exception e) {
                throw new JobExecutionException("Could not execute batch job", e);
            }
        }
    }
}