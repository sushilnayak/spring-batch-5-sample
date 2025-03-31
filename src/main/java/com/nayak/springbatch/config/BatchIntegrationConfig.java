package com.nayak.springbatch.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchRequestHandler;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;

import java.io.File;
import java.util.Date;

@EnableIntegration
@Configuration
@RequiredArgsConstructor
public class BatchIntegrationConfig {
    private final JobLauncher jobLauncher;
    private final Job job;

    @Bean
    public DirectChannel inputChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "inputChannel")
    public JobLaunchRequestHandler messageHandler() {
        return new JobLaunchingMessageHandler(jobLauncher);
    }

    @Bean
    @InboundChannelAdapter(channel = "fileChannel", poller = @Poller(fixedDelay = "5000"))
    public MessageSource<File> fileMessageSource() {
        FileReadingMessageSource fileReadingMessageSource = new FileReadingMessageSource();
        fileReadingMessageSource.setDirectory(new File("/input-path"));
        fileReadingMessageSource.setFilter(new SimplePatternFileListFilter("*.csv"));
        return fileReadingMessageSource;
    }

    @Bean
    @ServiceActivator(inputChannel = "fileChannel")
    public MessageHandler fileProcessor() {
        return message -> {
            File file = (File) message.getPayload();
            JobParameters params = new JobParametersBuilder()
                    .addString("filePath", file.getAbsolutePath())
                    .addDate("runDate", new Date())
                    .toJobParameters();

            Message<JobLaunchRequest> jobMessage =
                    MessageBuilder.withPayload(new JobLaunchRequest(job, params))
                            .build();

            inputChannel().send(jobMessage);
        };
    }
}
