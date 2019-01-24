package kkb.model;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

public class AccountTest {

	@Test
	public void test() {
		Account account = new Account(new Long(1), new Long(2), new Date());  
		assertNotNull(account);
	}

}
