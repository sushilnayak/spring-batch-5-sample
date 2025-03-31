package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;

@Configuration
@EnableBatchProcessing // With Spring Boot 3.x, @EnableBatchProcessing is not required anymore as it's autoconfigured
@RequiredArgsConstructor
public class SimpleJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job simpleJob() {
        return new JobBuilder("simpleJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(simpleStep())
                .build();
    }

    @Bean
    public Step simpleStep() {
        return new StepBuilder("simpleStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Simple Step executed at " + LocalDateTime.now());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }


}
