package com.example.springbatchdemo.config;

import com.example.springbatchdemo.listenres.CustomJobExecutionListener;
import com.example.springbatchdemo.model.Watchlist;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.NoWorkFoundStepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class CsvFileToDatabaseJobConfig {

    @Bean
    Job job(JobBuilderFactory jbf, Step step) {
        return jbf.get("ETL")
                  .incrementer(new RunIdIncrementer())
                  .start(step)
                  .listener(customCustomJobExecutionListener())
                  .build();
    }

    @Bean
    public CustomJobExecutionListener customCustomJobExecutionListener() {
        return new CustomJobExecutionListener();
    }

    @Bean
    Step csvFileToDatabaseStep(StepBuilderFactory sbf) {
        return sbf.get("file-db")
                .<Watchlist, Watchlist>chunk(100)
                .reader(fileReader(null))
                .processor(process(null, null))
                .writer(jdbcWriter(null))
                .listener(new NoWorkFoundStepExecutionListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<Watchlist> fileReader(@Value("${csv.to.database.job.source.file.path}") Resource in) {

        return new FlatFileItemReaderBuilder<Watchlist>()
                .name("file-reader")
                .resource(in)
                .targetType(Watchlist.class)
                .linesToSkip(1)
                .delimited()
                .delimiter(",")
                .names(new String[]{"id", "firstName1", "middleName1", "lastName1", "firstName2", "middleName2", "lastName2", "birthDate1", "birthDate2", "birthDate3",})
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<Watchlist, Watchlist> process(@Value("#{jobParameters[type]}") String type, @Value("#{jobParameters[ts]}") Long ts) {
        return new ItemProcessor<Watchlist, Watchlist>() {
            @Override
            public Watchlist process(Watchlist watchlist) {
                watchlist.setType(type);
                watchlist.setTs(ts);
                return watchlist;
            }
        };
    }

    @Bean
    public JdbcBatchItemWriter<Watchlist> jdbcWriter(DataSource ds) {
        return new JdbcBatchItemWriterBuilder<Watchlist>()
                .dataSource(ds)
                .sql("INSERT INTO WATCHLIST(type, id, firstName1, middleName1, lastName1, firstName2, middleName2, lastName2, birthDate1, birthDate2, birthDate3, ts) VALUES (:type, :id, :firstName1, :middleName1, :lastName1, :firstName2, :middleName2, :lastName2, :birthDate1, :birthDate2, :birthDate3, :ts)")
                .beanMapped()
                .build();
    }
}
