package com.example.springbatchdemo;

import com.example.springbatchdemo.config.CsvFileToDatabaseJobConfig;
import com.example.springbatchdemo.config.TestConfiguration;
import com.example.springbatchdemo.listenres.CustomJobExecutionListener;
import com.example.springbatchdemo.repositories.WatchlistRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class, CsvFileToDatabaseJobConfig.class, WatchlistRepository.class, CustomJobExecutionListener.class})
@TestPropertySource(properties = {
        "csv.to.database.job.source.file.path=classpath:watchlist100.csv"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class SpringBatchDemoApplicationMissingColumnTests {

    @Autowired
    JobBuilderFactory jbf;

    @Autowired
    StepBuilderFactory sbf;

    @Autowired
    CustomJobExecutionListener customJobExecutionListener;

    @Autowired
    private DataSource testDataSource;

    @Autowired
    private JdbcBatchItemWriter jdbcBatchItemWriter;


    @Autowired
    private Job job;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void shouldReturnFailedWhenMissingColumn() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        // given
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLong("ts", System.currentTimeMillis());

        jobParametersBuilder.addString("type", "DENY");
        JobParameters jobParameterWithDENYType = jobParametersBuilder.toJobParameters();
        // when
        JobExecution jobExecution = jobLauncher.run(job, jobParameterWithDENYType);

        // then
        assertThat(jobExecution.getExitStatus()
                               .getExitCode()).isEqualToIgnoringCase("FAILED");
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM WATCHLIST WHERE type = ?", new Object[]{"DENY"}, Integer.class)).isEqualTo(0);

    }
}
