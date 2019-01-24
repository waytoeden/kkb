package kkb.producer;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

import kkb.model.*;

public class BankLogTest {

	@Test
	public void testInitLog() {
		assertNotNull(BankLog.getInstance());
	}

	@Test
	public void testGetInstance() {
		assertNotNull(BankLog.getInstance());
	}

	@Test
	public void testRegCustomer() {
		assertNotNull(BankLog.getInstance().regCustomer(new Customer(0,"",new Date()))); 
	}

	@Test
	public void testOpenAccount() {
		assertNotNull(BankLog.getInstance().openAccount(new Customer(0,"",new Date()), new Account(0,0,new Date()))); 
	}

	@Test
	public void testDeposit() {
		assertNotNull(BankLog.getInstance().deposit(new Customer(0,"",new Date()), new Account(0,0,new Date()), 0, new Date()));
	}

	@Test
	public void testWithdraw() {
		assertNotNull(BankLog.getInstance().withdraw(new Customer(0,"",new Date()), new Account(0,0,new Date()), 0, new Date()));
	}

	@Test
	public void testRemit() {
		assertNotNull(BankLog.getInstance().withdraw(new Customer(0,"",new Date()), new Account(0,0,new Date()), 0, new Date()));
	}

}
