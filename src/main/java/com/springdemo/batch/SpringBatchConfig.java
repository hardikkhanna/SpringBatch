package com.springdemo.batch;

import java.beans.PropertyVetoException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.springdemo.batch.model.Person;
import com.springdemo.batch.step.JobCompletionListener;
import com.springdemo.batch.step.PersonConverter;
import com.springdemo.batch.step.PersonItenProcessor;
import com.springdemo.batch.step.PersonPreparedStatementSetter;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

	private static final String INSERTQUERY = "INSERT INTO person(person_id,first_name,last_name,email,age) VALUES(?,?,?,?,?)";

	private Logger logger = Logger.getLogger(getClass().getName());

	@Autowired
	private Environment env;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource datasource;

	
	// Concurrency Limit identifies the maximum number of parallel accesses allowed
	// bydefualt it is set to 
	@Bean
	public TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor("spring_batch");
		asyncTaskExecutor.setConcurrencyLimit(5);
		return asyncTaskExecutor;
	}

	@Bean
	public PersonItenProcessor processor() {
		return new PersonItenProcessor();
	}

	// create a Bean to read the data from the XML file
	// TO-DO have a glance at flatfilereader to know the difference
	// go through various Reader once if have time
	@Bean
	public StaxEventItemReader<Person> reader() {
		Map<String, Class> aliases = new HashMap<>();
		aliases.put("person", Person.class);
		PersonConverter converter = new PersonConverter();
		XStreamMarshaller ummarshaller = new XStreamMarshaller();
		ummarshaller.setAliases(aliases);
		ummarshaller.setConverters(converter);
		StaxEventItemReader<Person> reader = new StaxEventItemReader<>();
		reader.setResource(new ClassPathResource("persons.xml"));
		reader.setFragmentRootElementName("person");
		reader.setUnmarshaller(ummarshaller);
		return reader;
	}

	
	// Created a bean to read data from flat file or CSV file 
	// was'nt able to read thread based data from xml 
	// ask anyone if they were able to process that piece of code.
	@Bean
	public LineMapper<Person> lineMapper() {
		DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<Person>();
		lineMapper.setLineTokenizer(new DelimitedLineTokenizer() {
			{
				setNames(new String[] { "personId", "firstName", "lastName", "email", "age" });
			}
		});
		lineMapper.setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
			{
				setTargetType(Person.class);
			}
		});
		return lineMapper;
	}

	@Bean
	public FlatFileItemReader<Person> csvReader() {
		return new FlatFileItemReaderBuilder<Person>().name("studentItemReader")
				.resource(new ClassPathResource("persons.csv")).lineMapper(lineMapper()).linesToSkip(1).build();
	}

	// create a bean to write the data in database .
	// TO-DO - study about the various Step Writer used to write the data
	// also look for Cursor implementation if have time
	@Bean
	public JdbcBatchItemWriter<Person> writer() {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
		writer.setDataSource(myDataSource());
		writer.setSql(INSERTQUERY);
		writer.setItemPreparedStatementSetter(new PersonPreparedStatementSetter());
		return writer;
	}

	// to add Multithreaded step we need to add Asynchronous task executor it will
	// create chunks of data and threads will be created based on that.
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Person, Person>chunk(1000).reader(csvReader()).processor(processor())
				.writer(writer()).build();
	}

	/*
	 * @Bean Step step2() { return stepBuilderFactory.get("step2").<Person,
	 * Person>chunk(1000).reader(reader()).processor(processor())
	 * .writer(csvWriter()).build(); }
	 */

	@Bean
	public Job exportPerosnJob() {
		return jobBuilderFactory.get("importPersonJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(step1()).end().build();
	}

	// create a bean for jobexecutioner to let us know the batch status Failed or
	// passed
	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionListener();
	}

	// Bean to define datasource
	// TO-DO need to add hibernate properties also not-necessary but for knowledge
	@Bean
	public DataSource myDataSource() {

		// create connection pool
		ComboPooledDataSource myDataSource = new ComboPooledDataSource();

		// set the jdbc driver
		try {
			myDataSource.setDriverClass(env.getProperty("spring.datasource.driver-class-name"));
		} catch (PropertyVetoException exc) {
			throw new RuntimeException(exc);
		}

		// for sanity's sake, let's log url and user ... just to make sure we are
		// writting the data
		logger.info("jdbc.url=" + env.getProperty("spring.datasource.url"));
		logger.info("jdbc.user=" + env.getProperty("spring.datasource.username"));

		// set database connection props
		myDataSource.setJdbcUrl(env.getProperty("spring.datasource.url"));
		myDataSource.setUser(env.getProperty("spring.datasource.username"));
		myDataSource.setPassword(env.getProperty("spring.datasource.password"));

		return myDataSource;
	}
}
