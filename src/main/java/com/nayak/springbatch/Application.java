package com.nayak.springbatch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Date;

@RequiredArgsConstructor
@SpringBootApplication
public class Application implements CommandLineRunner, ApplicationRunner {
    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;
    private final Job job;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    //=================================================
    // Command Line way of triggering Job
    //=================================================
    @Override
    public void run(String... args) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addDate("runDate", new Date())
                .toJobParameters();
        jobLauncher.run(job, params);
    }

    //=================================================
    // Application Argument way of triggering Job
    // java -jar app.jar --job=importJob
    //=================================================
    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (args.containsOption("job")) {
            String jobName = args.getOptionValues("job").get(0);
            Job job = jobRegistry.getJob(jobName);

            JobParameters parameters = new JobParametersBuilder().addDate("runDate", new Date()).toJobParameters();

            jobLauncher.run(job, parameters);
        }
    }
}

