package io.bdrc.gittodbs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.jena.ext.com.google.common.io.Files;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFWriter;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
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
	private static final String EX = "http://example.com/";
	private static Dataset ds;
	
	@BeforeClass
	public static void init() throws IOException {
	    ds = DatasetFactory.createGeneral();
	    FusekiHelpers.fu = DatasetAccessorFactory.create(ds);
	    InMemoryCouchDb couchDbClient = new InMemoryCouchDb();
        couchDbClient.createDatabase("users");
        StdHttpClient stdHttpClient = new StdHttpClient(couchDbClient);
        CouchHelpers.httpClient = stdHttpClient;
        StdCouchDbInstance stdCouchDbInstance = new StdCouchDbInstance(stdHttpClient);
        CouchHelpers.dbInstance = stdCouchDbInstance;
        CouchHelpers.testMode = true;
        CouchHelpers.putDB(DocType.TEST);
        tempDir = Files.createTempDir();
        System.out.println("create temporary directory for git testing in "+tempDir.getAbsolutePath());
        GitToDB.gitDir = tempDir.getAbsolutePath()+'/';
        GitHelpers.createGitRepo(DocType.TEST);
        GitHelpers.ensureGitRepo(DocType.TEST);
	}
	
	public static void deleteRec(File f) throws IOException {
	    if (f.isDirectory()) {
	      for (File c : f.listFiles())
	          deleteRec(c);
	    }
	    if (!f.delete())
	      throw new FileNotFoundException("Failed to delete file: " + f);
	  }
	
	@AfterClass
	public static void finish() throws IOException {
	    deleteRec(tempDir);
	}
	
	public static String writeModelToGitPath(Model m, String path) throws NoFilepatternException, GitAPIException {
	    RDFWriter.create().source(m.getGraph()).lang(RDFLanguages.TTL).build().output(GitToDB.gitDir+"tests/"+path);
	    Repository r = GitHelpers.typeRepo.get(DocType.TEST); 
        Git git = new Git(r);
        RevCommit commit;
        git.add().addFilepattern(path).call();
        commit = git.commit().setMessage("adding "+path).call();
        git.close();
        return RevCommit.toString(commit);
	}
	
	@Test
	public void test1() throws NoFilepatternException, GitAPIException, TimeoutException, InterruptedException {
	    Model m = ModelFactory.createDefaultModel();
	    Resource r1 = m.createResource(EX+"r1");
	    Property p1 = m.createProperty(EX, "p1");
	    Resource r2 = m.createResource(EX+"r2");
	    m.add(r1, p1, r2);
	    String rev = writeModelToGitPath(m, "r1.ttl");
	    assertTrue(GitHelpers.getHeadRev(DocType.TEST).equals(rev));
	    assertTrue(GitHelpers.getLastRefOfFile(DocType.TEST, "r1.ttl").equals(rev));
	    TransferHelpers.syncTypeCouch(DocType.TEST);
	    Model couchM = CouchHelpers.getModelFromDocId("bdr:r1", DocType.TEST);
	    assertTrue(couchM.isIsomorphicWith(m));
	    TransferHelpers.syncTypeFuseki(DocType.TEST);
	    Model fusekiM = FusekiHelpers.getModel(TransferHelpers.BDR+"r1");
	    assertTrue(fusekiM.isIsomorphicWith(m));
	    Property p2 = m.createProperty(EX, "p2");
	    m = ModelFactory.createDefaultModel();
	    m.add(r2, p2, r1);
	    String newRev = writeModelToGitPath(m, "r2.ttl");
	    System.out.println("just committed "+newRev);
	    assertTrue(GitHelpers.getHeadRev(DocType.TEST).equals(newRev));
        assertTrue(GitHelpers.getLastRefOfFile(DocType.TEST, "r1.ttl").equals(rev));
        TransferHelpers.syncTypeFuseki(DocType.TEST);
        fusekiM = FusekiHelpers.getModel(TransferHelpers.BDR+"r2");
        assertTrue(fusekiM.isIsomorphicWith(m));
	}
	
	@Test
	public void test2() {
	    Model forcedSync = ModelFactory.createDefaultModel();
	    Resource res = forcedSync.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfoTest");
        Property p = forcedSync.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        Literal l = forcedSync.createLiteral("abc");
        forcedSync.add(res, p, l);
        FusekiHelpers.fu.putModel(TransferHelpers.ADMIN_PREFIX+"system", forcedSync);
        FusekiHelpers.initSyncModel();
        Model fusekiSync = FusekiHelpers.getSyncModel();
        assertTrue(fusekiSync.isIsomorphicWith(forcedSync));
	}
}
