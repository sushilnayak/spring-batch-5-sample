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

import java.util.Random;

@Configuration
@RequiredArgsConstructor
public class ConditionalJobConfig {
    private final PlatformTransactionManager transactionManager;
    private final JobRepository jobRepository;

    @Bean
    public Job conditionalJob() {
        return new JobBuilder("conditionalJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(conditionalStep()).on("COMPLETED").to(successStep())
                .from(conditionalStep()).on("FAILED").to(failureStep())
                .from(conditionalStep()).on("*").to(unknownStep())
                .end()
                .build();
    }

    public Step conditionalStep() {
        return new StepBuilder("conditionalStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Conditional step executing...");
                    // Simulate random success/failure
                    if (new Random().nextBoolean()) {
                        System.out.println("Step completed successfully");
                        return RepeatStatus.FINISHED;
                    } else {
                        System.out.println("Step failed");
                        throw new RuntimeException("Step failed randomly");
                    }
                }, transactionManager)
                .build();
    }

    @Bean
    public Step successStep() {
        return new StepBuilder("successStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Success path executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step failureStep() {
        return new StepBuilder("failureStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Failure path executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step unknownStep() {
        return new StepBuilder("unknownStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Unknown status path executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
