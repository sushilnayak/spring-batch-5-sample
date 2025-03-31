package com.nayak.springbatch.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class BatchKafkaConfig {
    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "job-triggers", groupId = "batch-job-consumer")
    public void listen(String message) throws Exception {
        JobTriggerDto jobTriggerDto = objectMapper.readValue(message, JobTriggerDto.class);

        Job job = jobRegistry.getJob(jobTriggerDto.jobName());

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.now());

        jobTriggerDto.parameters().forEach((k, v) -> {
            jobParametersBuilder.addString(k, v.toString());
        });

        jobLauncher.run(job, jobParametersBuilder.toJobParameters());

    }

    public static record JobTriggerDto(String jobName, String jobGroup, Map<String, Object> parameters) {
    }
}
