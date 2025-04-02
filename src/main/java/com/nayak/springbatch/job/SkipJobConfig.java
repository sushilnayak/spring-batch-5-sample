package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@RequiredArgsConstructor
@Configuration
public class SkipJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job skipJob() {
        return new JobBuilder("skipJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(skipStep())
                .build();
    }

    @Bean
    public Step skipStep() {
        return new StepBuilder("skipStep", jobRepository)
                .<Integer, Integer>chunk(10, transactionManager)
                .reader(new ItemReader<Integer>() {
                    private int count = 0;

                    @Override
                    public Integer read() {
                        if (count < 30) {
                            return count++;
                        }
                        return null; // Signals end of data
                    }
                })
                .processor(new ItemProcessor<Integer, Integer>() {
                    @Override
                    public Integer process(Integer item) {
                        // Skip multiples of 5 (will throw exception)
                        if (item % 5 == 0) {
                            System.out.println("Invalid item found: " + item);
                            throw new IllegalArgumentException("Invalid item: " + item);
                        }
                        return item * 2;
                    }
                })
                .writer(items -> {
                    for (Integer item : items) {
                        System.out.println("Writing processed item: " + item);
                    }
                })
                .faultTolerant()
                .skip(IllegalArgumentException.class) // Skip these exceptions
                .skipLimit(10) // Maximum items to skip
                .skipPolicy(customSkipPolicy()) // Optional custom skip policy
                .build();
    }
}
