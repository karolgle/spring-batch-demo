package com.example.springbatchdemo.listenres;

import com.example.springbatchdemo.repositories.WatchlistRepository;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Job Execution listener used to run sql delete query for old records after current job is completed successfully.
 */
@Service
public class CustomJobExecutionListener implements JobExecutionListener {

    @Autowired
    WatchlistRepository watchlistRepository;

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getExitStatus().getExitCode().equals("COMPLETED")) {
            watchlistRepository.deleteByTypeAndLessThenTS(jobExecution.getJobParameters()
                                                                      .getString("type"), jobExecution.getJobParameters()
                                                                                                      .getLong("ts"));
        }
    }
}
