package com.nayak.springbatch.service;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class JobService {
    private final JobLauncher jobLauncher;
    private final Job job;

    public JobExecution runJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("jobName", job.getName())
                .addString("jobId", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        return jobLauncher.run(job, jobParameters);
    }
}
