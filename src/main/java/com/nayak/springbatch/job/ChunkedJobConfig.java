package com.nayak.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class ChunkedJobConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Bean
    public Job chunkedJob() {
        return new JobBuilder("chunkedJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(chunkedStep())
                .build();
    }

    @Bean
    public Step chunkedStep() {
        return new StepBuilder("chunkedStep", jobRepository)
                .<Person, Person>chunk(5, transactionManager)
                .reader(reader())
                .processor(personItemProcessor())
                .writer(personItemWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("reader")
                .resource(new ClassPathResource("persons.txt"))
                .delimited()
                .names("firstName", "lastName")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> personItemProcessor() {
        return person -> {
            final String firstName = person.firstName().toUpperCase();
            final String lastName = person.lastName().toUpperCase();

            final Person transformedPerson = new Person(firstName, lastName);
            System.out.println("Converting (" + person + ") into (" + transformedPerson + ")");

            return transformedPerson;
        };
    }

    @Bean
    public JdbcBatchItemWriter<Person> personItemWriter() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .beanMapped()
                .build();
    }

    record Person(String firstName, String lastName) {
    }
}
