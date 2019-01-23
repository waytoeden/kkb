package kkb.producer;

import static org.junit.Assert.*;

import org.junit.Test;

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
		fail("Not yet implemented");
	}

	@Test
	public void testOpenAccount() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeposit() {
		fail("Not yet implemented");
	}

	@Test
	public void testWithdraw() {
		fail("Not yet implemented");
	}

	@Test
	public void testRemit() {
		fail("Not yet implemented");
	}

}
