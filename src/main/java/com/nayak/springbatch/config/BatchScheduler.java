package com.nayak.springbatch.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDate;

@Configuration
@RequiredArgsConstructor
public class BatchScheduler {
    private final JobLauncher jobLauncher;
    private final Job job;

    //==============================================
    // Scheduled way of triggering job
    //==============================================
    @Scheduled(cron = "0 0 * * * *")
    public void runReportEveryHour() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.now())
                .toJobParameters();
        jobLauncher.run(job, jobParameters);
    }
}
