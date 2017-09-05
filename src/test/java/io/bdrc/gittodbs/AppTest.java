package io.bdrc.gittodbs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.jena.ext.com.google.common.io.Files;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.DatasetFactory;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import io.bdrc.gittodbs.TransferHelpers.DocType;
import mcouch.core.InMemoryCouchDb;

/**
 * Unit test for simple App.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AppTest 
{
	private static File tempDir;
	
	@BeforeClass
	public static void init() throws IOException {
	    Dataset ds = DatasetFactory.createGeneral();
	    FusekiHelpers.fu = DatasetAccessorFactory.create(ds);
	    InMemoryCouchDb couchDbClient = new InMemoryCouchDb();
        couchDbClient.createDatabase("users");
        StdHttpClient stdHttpClient = new StdHttpClient(couchDbClient);
        CouchHelpers.httpClient = stdHttpClient;
        StdCouchDbInstance stdCouchDbInstance = new StdCouchDbInstance(stdHttpClient);
        CouchHelpers.dbInstance = stdCouchDbInstance;
        CouchHelpers.putDB(DocType.TEST);
        tempDir = Files.createTempDir();
        System.out.println("create temporary directory for git testing in "+tempDir.getAbsolutePath());
        GitToDB.gitDir = tempDir.getAbsolutePath()+'/';
        GitHelpers.createGitRepo(DocType.TEST);
        GitHelpers.ensureGitRepo(DocType.TEST);
	}
	
	@AfterClass
	public static void finish() {
	    tempDir.delete();
	}
	
	@Test
	public void test1() {
	    assertTrue(true);
	}
}
