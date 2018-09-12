package com.example.springbatchdemo;

import com.example.springbatchdemo.config.CsvFileToDatabaseJobConfig;
import com.example.springbatchdemo.config.TestConfiguration;
import com.example.springbatchdemo.listeners.CustomJobExecutionListener;
import com.example.springbatchdemo.model.WatchlistType;
import com.example.springbatchdemo.repositories.WatchlistRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class, CsvFileToDatabaseJobConfig.class, WatchlistRepository.class, CustomJobExecutionListener.class})
@TestPropertySource(properties = {
        "csv.to.database.job.source.file.path=classpath:watchlist100.csv"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class SpringBatchDemoApplicationMissingColumnTests {

    @Autowired
    private Job job;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void shouldReturnFailedWhenMissingColumn() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        // given
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLong("ts", System.currentTimeMillis());

        jobParametersBuilder.addString("type", WatchlistType.DENY.toString());
        JobParameters jobParameterWithDENYType = jobParametersBuilder.toJobParameters();
        // when
        JobExecution jobExecution = jobLauncher.run(job, jobParameterWithDENYType);

        // then
        assertThat(jobExecution.getExitStatus()
                               .getExitCode()).isEqualToIgnoringCase("FAILED");
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM WATCHLIST WHERE type = ?", new Object[]{WatchlistType.DENY.toString()}, Integer.class)).isEqualTo(0);

    }
}
