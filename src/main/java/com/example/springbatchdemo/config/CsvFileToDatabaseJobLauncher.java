package com.example.springbatchdemo.config;

import com.example.springbatchdemo.model.WatchlistType;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class CsvFileToDatabaseJobLauncher {

    private final Job job;

    private final JobLauncher jobLauncher;

    @Autowired
    CsvFileToDatabaseJobLauncher(Job job, JobLauncher jobLauncher) {
        this.job = job;
        this.jobLauncher = jobLauncher;
    }

    @Scheduled(cron = "${csv.to.database.job.cron}")
    void launchCsvFileToDatabaseJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        //run job with parameters
        jobLauncher.run(job, getJobParameters());
    }

    private JobParameters getJobParameters() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();

        //timestamp for the job should be the same across all data chunks
        jobParametersBuilder.addLong("ts", System.currentTimeMillis());

        //get random type of List
        jobParametersBuilder.addString("type", WatchlistType.getRandom()
                                                            .toString());
        return jobParametersBuilder.toJobParameters();
    }
}
