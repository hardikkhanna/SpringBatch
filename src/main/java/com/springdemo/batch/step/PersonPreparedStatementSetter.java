package com.springdemo.batch.step;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import com.springdemo.batch.model.Person;

public class PersonPreparedStatementSetter implements ItemPreparedStatementSetter<Person> {

	private Logger logger = Logger.getLogger(getClass().getName());
	
	@Override
	public void setValues(Person person, PreparedStatement ps) throws SQLException {
		ps.setLong(1, Long.valueOf(person.getPersonId()));
		ps.setString(2, person.getFirstName());
		ps.setString(3, person.getLastName());
		ps.setString(4, person.getEmail());
		ps.setInt(5, Integer.valueOf(person.getAge()));
		logger.info("----------------> Inside process : " + person.toString());
	}

}
