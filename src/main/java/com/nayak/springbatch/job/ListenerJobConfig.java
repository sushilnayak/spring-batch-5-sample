package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@RequiredArgsConstructor
@Configuration
public class ListenerJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job listenerJob() {
        return new JobBuilder("listenerJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(new org.springframework.batch.core.JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        System.out.println("Before Job: " + jobExecution.getJobInstance().getJobName());
                        System.out.println("Job parameters: " + jobExecution.getJobParameters());
                        jobExecution.getExecutionContext().put("jobStartTime", System.currentTimeMillis());
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        long startTime = jobExecution.getExecutionContext().getLong("jobStartTime", 0);
                        System.out.println("After Job: " + jobExecution.getJobInstance().getJobName());
                        System.out.println("Status: " + jobExecution.getStatus());
                        System.out.println("Duration: " + (System.currentTimeMillis() - startTime) + "ms");
                    }
                })
                .start(listenerStep())
                .build();
    }

    @Bean
    public Step listenerStep() {
        return new StepBuilder("listenerStep", jobRepository)
                .<Integer, Integer>chunk(5, transactionManager)
                .reader(new ItemReader<>() {
                    private int count = 0;

                    @Override
                    public Integer read() {
                        return count < 15 ? count++ : null;
                    }
                })
                .processor(item -> item * 10)
                .writer(items -> {
                    for (Integer item : items) {
                        System.out.println("Writing: " + item);
                    }
                })
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println("Before Step: " + stepExecution.getStepName());
                        stepExecution.getExecutionContext().put("stepStartTime", System.currentTimeMillis());
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        long startTime = stepExecution.getExecutionContext().getLong("stepStartTime", 0);
                        System.out.println("After Step: " + stepExecution.getStepName());
                        System.out.println("Read count: " + stepExecution.getReadCount());
                        System.out.println("Write count: " + stepExecution.getWriteCount());
                        System.out.println("Step Duration: " + (System.currentTimeMillis() - startTime) + "ms");
                        return ExitStatus.COMPLETED;
                    }
                })
                .listener(new ItemReadListener<>() {
                    @Override
                    public void beforeRead() {
                        System.out.println("Before reading an item");
                    }

                    @Override
                    public void afterRead(Integer item) {
                        System.out.println("After reading item: " + item);
                    }

                    @Override
                    public void onReadError(Exception ex) {
                        System.out.println("Error reading item: " + ex.getMessage());
                    }
                })
                .build();
    }
}
