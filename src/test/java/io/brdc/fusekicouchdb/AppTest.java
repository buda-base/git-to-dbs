package io.brdc.fusekicouchdb;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

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
		TransferHelpers.transferOntology();
		TransferHelpers.transferCompleteDB();
		//TransferHelpers.transferOneDoc("plc:G00AG01618");
		assertTrue(true);
    }

}
