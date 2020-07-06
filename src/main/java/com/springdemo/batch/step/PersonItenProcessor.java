package com.springdemo.batch.step;

import org.springframework.batch.item.ItemProcessor;

import com.springdemo.batch.model.Person;


public class PersonItenProcessor implements ItemProcessor<Person, Person>{

	@Override
	public Person process(Person person) throws Exception {
		return person;
	}
}
