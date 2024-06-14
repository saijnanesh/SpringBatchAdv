package com.example.spring.batch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.spring.batch.model.Product;

@Configuration
public class BatchConfig {

	@Autowired
	DataSource dataSource;

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private PlatformTransactionManager platformTransactionManager;

	@Bean
	public Job getJob() {
		return new JobBuilder("job-1", jobRepository).flow(getStep()).end().build();
	}

	@Bean
	public Step getStep() {
		StepBuilder builder = new StepBuilder("step-1", jobRepository);
		return builder.<Product, Product>chunk(2, platformTransactionManager).reader(reader()).processor(processor())
				.writer(writer()).build();
	}

	@Bean
	public ItemReader<Product> reader() {
		// read the csv file
		FlatFileItemReader<Product> reader = new FlatFileItemReader<>();
		reader.setResource(new ClassPathResource("Products.csv"));
		// read the columns in csv in the form of line
		DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();
		// set the tokenizer to tell that 1st field in csv go to id,2nd to name and so
		// on..
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames("id", "name", "description", "price");
		// set a field mapper to parse the tokenizer into object fields
		BeanWrapperFieldSetMapper<Product> fieldMapper = new BeanWrapperFieldSetMapper<>();
		fieldMapper.setTargetType(Product.class);
		// set the tokenizer and field mapper to line mapper
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fieldMapper);
		// set the line mapper to reader
		reader.setLineMapper(lineMapper);
		// return reader
		return reader;
	}

	@Bean
	public ItemProcessor<Product, Product> processor() {
		return (p) -> {
			p.setPrice(p.getPrice() - ((p.getPrice() * 10) / 100));
			return p;
		};
	}

	@Bean
	public ItemWriter<Product> writer() {
		JdbcBatchItemWriter<Product> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
		jdbcBatchItemWriter.setDataSource(dataSource);
		jdbcBatchItemWriter
				.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Product>());
		jdbcBatchItemWriter
				.setSql("insert into Product (id,name,description,price) values (:id,:name,:description,:price)");
		return jdbcBatchItemWriter;
	}
}
