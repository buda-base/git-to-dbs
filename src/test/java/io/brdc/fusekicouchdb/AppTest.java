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
		TransferHelpers.transferOneDoc("plc:G00AG01618");
		assertTrue(true);
    }

}
