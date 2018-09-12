package com.example.springbatchdemo;

import com.example.springbatchdemo.config.CsvFileToDatabaseJobConfig;
import com.example.springbatchdemo.config.TestConfiguration;
import com.example.springbatchdemo.listeners.CustomJobExecutionListener;
import com.example.springbatchdemo.model.WatchlistType;
import com.example.springbatchdemo.repositories.WatchlistRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class, CsvFileToDatabaseJobConfig.class, WatchlistRepository.class, CustomJobExecutionListener.class})
@TestPropertySource(properties = {
        "csv.to.database.job.source.file.path=classpath:watchlist100000.csv"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class SpringBatchDemoApplicationTests {

    private static final String COMPLETED = "COMPLETED";
    @Autowired
    JobBuilderFactory jbf;

    @Autowired
    StepBuilderFactory sbf;

    @Autowired
    CustomJobExecutionListener customJobExecutionListener;

    @Autowired
    private Job job;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private JobParameters jobParameterWithPEPType;
    private JobParameters jobParameterWithAMType;
    private JobParameters jobParameterWithDENYType;

    @Before
    public void setUp() {
        long ts = System.currentTimeMillis();
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLong("ts", ts);

        jobParametersBuilder.addString("type", WatchlistType.PEP.toString());
        jobParameterWithPEPType = jobParametersBuilder.toJobParameters();

        jobParametersBuilder.addString("type", WatchlistType.AM.toString());
        jobParameterWithAMType = jobParametersBuilder.toJobParameters();

        jobParametersBuilder.addString("type", WatchlistType.DENY.toString());
        jobParameterWithDENYType = jobParametersBuilder.toJobParameters();

    }

    @Test
    public void shouldLoad100kRowsWithPEPType() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        // given
        // when
        JobExecution jobExecution = jobLauncher.run(job, jobParameterWithPEPType);

        // then
        assertThat(jobExecution.getExitStatus()
                               .getExitCode()).isEqualToIgnoringCase(COMPLETED);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM WATCHLIST WHERE type = ?", new Object[]{WatchlistType.PEP.toString()}, Integer.class)).isEqualTo(100000);

    }

    @Test
    public void shouldLoad300kRowsWithAllTypes() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        // given
        List<JobExecution> finishedJobs = new ArrayList<>();
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("types", Stream.of(WatchlistType.PEP.toString(), WatchlistType.AM.toString(), WatchlistType.DENY.toString())
                                           .collect(Collectors.toSet()));
        // when
        finishedJobs.add(jobLauncher.run(job, jobParameterWithPEPType));
        finishedJobs.add(jobLauncher.run(job, jobParameterWithAMType));
        finishedJobs.add(jobLauncher.run(job, jobParameterWithDENYType));

        // then
        assertThat(finishedJobs).extracting(jobExecution -> jobExecution.getExitStatus()
                                                                        .getExitCode())
                                .allMatch(s -> s.equalsIgnoreCase(COMPLETED));
        assertThat(namedParameterJdbcTemplate.queryForObject("SELECT count(*) FROM WATCHLIST WHERE type IN (:types)", parameters, Integer.class)).isEqualTo(300000);
    }
}
