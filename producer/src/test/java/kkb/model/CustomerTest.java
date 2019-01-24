package kkb.model;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

public class CustomerTest {

	@Test
	public void test() {
		Customer customer = new Customer(0,"dummy",new Date());  
		assertNotNull(customer);
	}

}
