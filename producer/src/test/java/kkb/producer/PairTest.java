package kkb.producer;

import static org.junit.Assert.*;

import org.junit.Test;

public class PairTest {

	@Test
	public void testPair() {
		Integer i = 10;
		String s = "dummy";
		Pair<Integer , String> pair = new Pair<>(i,s);
		assertSame(i, pair.first);
		assertSame(s, pair.second);
	}

}
