# Spring Batch 5 Samples

## Triggering Batch Job Examples

### 1. Using JobLauncher

[JobService.class](src/main/java/com/nayak/springbatch/service/JobService.java)

### 2. Rest Endpoint

[JobController.java](src/main/java/com/nayak/springbatch/controller/JobController.java)

### 3. Command Line Runner

[Application.java](src/main/java/com/nayak/springbatch/Application.java)

### 4. Application Argument

[Application.java](src/main/java/com/nayak/springbatch/Application.java)

### 5. Scheduled Jobs

[BatchScheduler.java](src/main/java/com/nayak/springbatch/config/BatchScheduler.java)

### 6. Quartz Scheduling triggering job

[QuartzConfig.java](src/main/java/com/nayak/springbatch/config/QuartzConfig.java)

### 7. Spring Integration triggering via Message

[BatchIntegrationConfig.java](src/main/java/com/nayak/springbatch/config/BatchIntegrationConfig.java)

### 8. Spring Integeration triggering via File Monitoring

### 9. Kafka consumer triggering job

[BatchKafkaConfig.java](src/main/java/com/nayak/springbatch/config/BatchKafkaConfig.java)

## Job/Step Configuration

### 1. Simple Job

[SimpleJobConfig.java](src/main/java/com/nayak/springbatch/job/SimpleJobConfig.java)

### 2. Multi-Step Job

[MultiStepConfig.java](src/main/java/com/nayak/springbatch/job/MultiStepConfig.java)

### 3. Conditional Job

[ConditionalJobConfig.java](src/main/java/com/nayak/springbatch/job/ConditionalJobConfig.java)

### 4. Decider Job

[DeciderJobConfig.java](src/main/java/com/nayak/springbatch/job/DeciderJobConfig.java)

### 5. Chunk Based Proceesing

[ChunkedJobConfig.java](src/main/java/com/nayak/springbatch/job/ChunkedJobConfig.java)

### 6. Parallel Job Processing

[ParallelJobConfig.java](src/main/java/com/nayak/springbatch/job/ParallelJobConfig.java)

### 7. Partitioned Job

[PartitionJobConfig.java](src/main/java/com/nayak/springbatch/job/PartitionJobConfig.java)

### 8. Retry Job - Step-level Retry

[RetryJobConfig.java](src/main/java/com/nayak/springbatch/job/RetryJobConfig.java)

### 9. Skip Logic Processing Job - Ignoring Problematic Records

[SkipJobConfig.java](src/main/java/com/nayak/springbatch/job/SkipJobConfig.java)

### 10. Listener Job

[ListenerJobConfig.java](src/main/java/com/nayak/springbatch/job/ListenerJobConfig.java)

## Failure/Retry Handling



