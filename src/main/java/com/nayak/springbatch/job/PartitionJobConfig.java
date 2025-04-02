package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Configuration
@RequiredArgsConstructor
public class PartitionJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job partitionJob() {
        return new JobBuilder("partitionJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(masterStep())
                .build();
    }

    @Bean
    public TaskExecutor asyncTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("spring_batch");
        executor.setConcurrencyLimit(10);
        return executor;
    }

    @Bean
    public Step masterStep() {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("slaveStep", partitioner())
                .step(slaveStep())
                .taskExecutor(asyncTaskExecutor())
                .gridSize(5)
                .build();
    }

    @Bean
    public Partitioner partitioner() {
        return gridSize -> {
            Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                context.put("partitionId", i);
                context.put("lowerBound", i * 100);
                context.put("upperBound", (i + 1) * 100 - 1);

                partitions.put("partition" + i, context);
            }

            return partitions;
        };
    }

    @Bean
    public Step slaveStep() {
        return new StepBuilder("slaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    ExecutionContext executionContext = chunkContext.getStepContext().getStepExecution().getExecutionContext();

                    int partitionId = executionContext.getInt("partitionId", -1);
                    int lowerBound = executionContext.getInt("lowerBound", -1);
                    int upperBound = executionContext.getInt("upperBound", -1);

                    System.out.println("Processing partition " + partitionId + " with range [" + lowerBound + "," + upperBound + "] on thread " + Thread.currentThread().getName());

                    Thread.sleep(ThreadLocalRandom.current().nextInt(500, 2000));

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
