package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.NetworkException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.CompositeRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.policy.TimeoutRetryPolicy;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Configuration
@RequiredArgsConstructor
public class RetryJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job retryJob() {
        return new JobBuilder("retryJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(retryStep())
                .build();
    }

    @Bean
    public Step retryStep() {
        return new StepBuilder("retryStep", jobRepository)
                .<String, String>chunk(10, transactionManager)
                .reader(new ItemReader<>() {
                    private final String[] items = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
                    private int count = 0;

                    @Override
                    public String read() {
                        if (count < items.length) {
                            return items[count++];
                        }
                        return null; // Signals end of data
                    }
                })
                .processor(item -> {
                    // Simulate random failures for items 'c' and 'h'
                    if ((item.equals("c") || item.equals("h")) && new Random().nextDouble() < 0.7) {
                        System.out.println("Processor failing for item: " + item);
                        throw new RuntimeException("Temporary failure processing item: " + item);
                    }
                    return item.toUpperCase();
                })
                .writer(items -> {
                    for (String item : items) {
                        System.out.println("Writing item: " + item);
                    }
                })
                .faultTolerant()
                .retry(RuntimeException.class)  // Specify which exceptions to retry
                .noRetry(FatalException.class)     // Specify which exceptions NOT to retry
                .retryLimit(3) // Maximum retry attempts
                .retryPolicy(customRetryPolicy()) // Optional custom retry policy
                .build();
    }

    @Bean
    public RetryPolicy customRetryPolicy() {
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(NetworkException.class, true);
        retryableExceptions.put(DatabaseException.class, true);

        // Time-based retry with exponential backoff
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);  // 1 second
        backOffPolicy.setMultiplier(2.0);        // Double the interval each time
        backOffPolicy.setMaxInterval(30000);     // Max 30 seconds wait

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(
                5,                     // max attempts
                retryableExceptions,   // retryable exceptions
                true,                  // default value
                true                   // traverse caused-by chain
        );

        CompositeRetryPolicy compositeRetryPolicy = new CompositeRetryPolicy();
        compositeRetryPolicy.setPolicies(new RetryPolicy[]{
                retryPolicy,
                new TimeoutRetryPolicy(60000)  // Overall timeout of 60 seconds
        });

        return compositeRetryPolicy;
    }
}
