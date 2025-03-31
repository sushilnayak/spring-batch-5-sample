package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class MultiStepConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public Job multiStepJob() {
        return new JobBuilder("multiStepJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .next(step2())
                .build();
    }

    @Bean
    public Step step1() {
        return new StepBuilder("step1", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Step1: Initialisation");

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step2() {
        return new StepBuilder("step2", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Step2: Processing");


                    Map<String, Object> jobParams = new HashMap<>();
                    jobParams.put("time", System.currentTimeMillis());
                    // Adding somethig random to the execution context which can be used by other following steps
                    chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("jobData", jobParams);

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step3() {
        return new StepBuilder("step3", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    // Accessing data from previous step
                    Object jobData = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().get("jobData");
                    System.out.println("Data from previous step: " + jobData);

                    System.out.println("Step3: Finishing");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
