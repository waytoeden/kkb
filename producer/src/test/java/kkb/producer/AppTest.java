package kkb.producer;

import static org.junit.Assert.*;

import org.junit.Test;

public class AppTest {

	@Test
	public void testMain() {
		App.main(null);
	}
	
	@Test
	public void TestLogThreadRun() {
		TestLogThread testTestLogThread = new TestLogThread();
		testTestLogThread.id = 1;
		testTestLogThread.run();
	}
	
	@Test
	public void EventIntvThreadRun() {
		EventIntvThread eventIntvThread = new EventIntvThread();
		eventIntvThread.run();
	}

}
