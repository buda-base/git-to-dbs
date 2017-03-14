package io.brdc.fusekicouchdb;

import io.brdc.fusekicouchdb.TransferHelpers;

import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
	
	@Before
	public void init() {
	}
	
	@Test
    public void testP1331()
    {
		TransferHelpers.transferOneDoc("crp:C1");
		assertTrue(true);
    }

}
