package io.bdrc.gittodbs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.jena.ext.com.google.common.io.Files;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.DatasetFactory;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.gittodbs.TransferHelpers.DocType;
import mcouch.core.InMemoryCouchDb;

/**
 * Unit test for simple App.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AppTest 
{
	private static File tempDir;
	private static final String BDR = TransferHelpers.BDR;
	private static final String BDO = TransferHelpers.BDO;
	private static Dataset ds;
	private static ObjectMapper om;
	
	@BeforeClass
	public static void init() throws IOException {
	    ds = DatasetFactory.createGeneral();
	    FusekiHelpers.fu = DatasetAccessorFactory.create(ds);
	    InMemoryCouchDb couchDbClient = new InMemoryCouchDb();
        couchDbClient.createDatabase("bdrc_test");
        StdHttpClient stdHttpClient = new StdHttpClient(couchDbClient);
        CouchHelpers.httpClient = stdHttpClient;
        StdCouchDbInstance stdCouchDbInstance = new StdCouchDbInstance(stdHttpClient);
        CouchHelpers.dbInstance = stdCouchDbInstance;
        CouchHelpers.testMode = true;
        CouchHelpers.putDB(DocType.TEST);
        tempDir = Files.createTempDir();
        JSONLDFormatter.typeToRootShortUri.put(DocType.TEST, "Test");
        System.out.println("create temporary directory for git testing in "+tempDir.getAbsolutePath());
        GitToDB.gitDir = tempDir.getAbsolutePath()+'/';
        GitHelpers.createGitRepo(DocType.TEST);
        GitHelpers.ensureGitRepo(DocType.TEST);
        Model baseModel = TransferHelpers.getOntologyBaseModel();
        BDRCReasoner.inferSymetry = true;
        TransferHelpers.bdrcReasoner = BDRCReasoner.getReasoner(baseModel);
        om = new ObjectMapper();
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
	
	public static Map<String, Object> objectFromJson(String path) throws JsonParseException, JsonMappingException, IOException {
	    ClassLoader classLoader = TransferHelpers.class.getClassLoader();
	    InputStream is = classLoader.getResourceAsStream(path);
	    File file = new File(classLoader.getResource(path).getFile());
	    return om.readValue(file, new TypeReference<Map<String, Object>>(){});
	    //return om.readTree(file);
	}
	
	@Test
	public void test1() throws NoFilepatternException, GitAPIException, TimeoutException, InterruptedException {
	    Model m = ModelFactory.createDefaultModel();
	    Resource r1 = m.createResource(BDR+"r1");
	    Property p1 = m.createProperty(BDO, "p1");
	    Resource r2 = m.createResource(BDR+"r2");
	    m.add(r1, p1, r2);
	    String rev = writeModelToGitPath(m, "r1.ttl");
	    assertTrue(GitHelpers.getHeadRev(DocType.TEST).equals(rev));
	    assertTrue(GitHelpers.getLastRefOfFile(DocType.TEST, "r1.ttl").equals(rev));
	    TransferHelpers.syncTypeCouch(DocType.TEST);
	    Model couchM = CouchHelpers.getModelFromDocId("bdr:r1", DocType.TEST);
	    System.out.println(couchM);
	    assertTrue(couchM.isIsomorphicWith(m));
	    TransferHelpers.syncTypeFuseki(DocType.TEST);
	    Model fusekiM = FusekiHelpers.getModel(BDR+"r1");
	    assertTrue(fusekiM.isIsomorphicWith(m));
	    Property p2 = m.createProperty(BDO, "p2");
	    m = ModelFactory.createDefaultModel();
	    m.add(r2, p2, r1);
	    String newRev = writeModelToGitPath(m, "r2.ttl");
	    System.out.println("just committed "+newRev);
	    assertTrue(GitHelpers.getHeadRev(DocType.TEST).equals(newRev));
        assertTrue(GitHelpers.getLastRefOfFile(DocType.TEST, "r1.ttl").equals(rev));
        TransferHelpers.syncTypeFuseki(DocType.TEST);
        fusekiM = FusekiHelpers.getModel(BDR+"r2");
        assertTrue(fusekiM.isIsomorphicWith(m));
	}
	
	@Test
	public void test2() throws IOException {
	    Model person = TransferHelpers.modelFromPath("P1583.ttl", DocType.PERSON);
	    Map<String,Object> res = LibFormat.objectFromModel(person, DocType.PERSON);
	    Map<String, Object> correct = objectFromJson("P1583.json");
	    assertTrue(correct.equals(res));
	    Model work = TransferHelpers.modelFromPath("WorkTestFPL.ttl", DocType.PERSON);
        res = LibFormat.objectFromModel(work, DocType.WORK);
        correct = objectFromJson("WorkTestFPL.json");
        assertTrue(correct.equals(res));
        Model outline = TransferHelpers.modelFromPath("OutlineTest.ttl", DocType.PERSON);
        res = LibFormat.objectFromModel(outline, DocType.WORK);
        correct = objectFromJson("OutlineTest.json");
        //System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(res));
        assertTrue(correct.equals(res));
	}
	
}
