package com.springdemo.batch.step;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.springdemo.batch.model.Person;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

public class PersonConverter implements Converter {

	@Override
	public boolean canConvert(Class type) {
		// TODO Auto-generated method stub
		return type.equals(Person.class);
	}

	@Override
	public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
		// TODO Auto-generated method stub
		reader.moveDown();
		Person person = new Person();
		long theRandomNum = (long) (Math.random() * Math.pow(10, 10));
		person.setPersonId(Long.valueOf(reader.getValue()) + theRandomNum);
		reader.moveUp();
		reader.moveDown();
		person.setFirstName(reader.getValue());
		reader.moveUp();
		reader.moveDown();
		person.setLastName(reader.getValue());
		reader.moveUp();
		reader.moveDown();
		person.setEmail(reader.getValue());
		reader.moveUp();
		reader.moveDown();
		person.setAge(Integer.valueOf(reader.getValue()));

		return reader;
	}

}
