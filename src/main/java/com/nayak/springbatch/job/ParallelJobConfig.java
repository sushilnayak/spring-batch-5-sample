package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ParallelJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public TaskExecutor asyncTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("spring_batch");
        executor.setConcurrencyLimit(10);
        return executor;
    }

    @Bean
    public Job parallelJob() {
        return new JobBuilder("parallelJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(parallelFlow1())
                .split(asyncTaskExecutor())
                .add(parallelFlow2(), parallelFlow3())
                .end()
                .build();
    }

    @Bean
    public Flow parallelFlow1() {
        return new FlowBuilder<SimpleFlow>("flow1")
                .start(new StepBuilder("flow1Step1", jobRepository) // STEP : flow1Step1
                        .tasklet((contribution, chunkContext) -> {
                            System.out.println("Flow 1, Step 1 executed on thread: " +
                                    Thread.currentThread().getName());
                            Thread.sleep(1000); // Simulate work
                            return RepeatStatus.FINISHED;
                        }, transactionManager)
                        .build())
                .build();
    }

    @Bean
    public Flow parallelFlow2() {
        return new FlowBuilder<SimpleFlow>("flow2")
                .start(new StepBuilder("flow2Step1", jobRepository) // STEP : flow2Step1
                        .tasklet((contribution, chunkContext) -> {
                            System.out.println("Flow 2, Step 1 executed on thread: " +
                                    Thread.currentThread().getName());
                            Thread.sleep(1500); // Simulate work
                            return RepeatStatus.FINISHED;
                        }, transactionManager)
                        .build())
                .next(new StepBuilder("flow2Step2", jobRepository)  // STEP : flow2Step2
                        .tasklet((contribution, chunkContext) -> {
                            System.out.println("Flow 2, Step 2 executed on thread: " +
                                    Thread.currentThread().getName());
                            Thread.sleep(500); // Simulate work
                            return RepeatStatus.FINISHED;
                        }, transactionManager)
                        .build())
                .build();
    }

    @Bean
    public Flow parallelFlow3() {
        return new FlowBuilder<SimpleFlow>("flow3")
                .start(new StepBuilder("flow3Step1", jobRepository)  // STEP : flow3Step1
                        .tasklet((contribution, chunkContext) -> {
                            System.out.println("Flow 3, Step 1 executed on thread: " +
                                    Thread.currentThread().getName());
                            Thread.sleep(2000); // Simulate work
                            return RepeatStatus.FINISHED;
                        }, transactionManager)
                        .build())
                .build();
    }

}
