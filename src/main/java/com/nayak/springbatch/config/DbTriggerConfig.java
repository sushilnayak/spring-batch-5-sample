package com.nayak.springbatch.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class DbTriggerConfig {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 10000)
    public void pollForTriggers() {
        List<Map<String, Object>> triggers = jdbcTemplate.queryForList("SELECT * FROM mi_consolidation WHERE status = 'PENDING'");

        for (Map<String, Object> trigger : triggers) {
            try {
                // Mark as in progress to prevent duplicate processing
                jdbcTemplate.update("UPDATE mi_consolidation SET status = 'IN_PROGRESS' WHERE id = ?", trigger.get("id"));

                // Get the job
                String jobName = (String) trigger.get("job_name");
                Job job = jobRegistry.getJob(jobName);

                // Build parameters
                JobParametersBuilder paramsBuilder = new JobParametersBuilder()
                        .addLong("triggerId", (Long) trigger.get("id"))
                        .addDate("triggerTime", new Date());

                // Get additional parameters if stored in another table or JSON
                String paramsJson = (String) trigger.get("parameters");
                if (paramsJson != null) {
                    Map<String, Object> params = objectMapper.readValue(
                            paramsJson, new TypeReference<Map<String, Object>>() {
                            });

                    for (Map.Entry<String, Object> entry : params.entrySet()) {
                        if (entry.getValue() instanceof String) {
                            paramsBuilder.addString(entry.getKey(), (String) entry.getValue());
                        }
                    }
                }

                // Run the job
                JobExecution execution = jobLauncher.run(job, paramsBuilder.toJobParameters());

                // Update trigger with execution ID
                jdbcTemplate.update("UPDATE mi_consolidation SET status = 'RUNNING', " + "job_execution_id = ? WHERE id = ?", execution.getId(), trigger.get("id"));

            } catch (Exception e) {
                // Mark as failed
                jdbcTemplate.update("UPDATE mi_consolidation SET status = 'FAILED', " + "error_message = ? WHERE id = ?", e.getMessage(), trigger.get("id"));
            }
        }
    }
}