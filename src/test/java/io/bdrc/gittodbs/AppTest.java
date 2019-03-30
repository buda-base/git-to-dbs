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
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFWriter;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.gittodbs.TransferHelpers.DocType;

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
//        FusekiHelpers.fu = DatasetAccessorFactory.create(ds);
        FusekiHelpers.fuConn = RDFConnectionFactory.connect(ds);
	    // for some reason, using both DataAccessor and RDFConnection on the same dataset doesn't work
//        FusekiHelpers.useRdfConnection = true;
//        FusekiHelpers.useRdfConnection = false;
        tempDir = Files.createTempDir();
        JSONLDFormatter.typeToRootShortUri.put(DocType.TEST, "Test");
        System.out.println("create temporary directory for git testing in "+tempDir.getAbsolutePath());
        GitToDB.gitDir = tempDir.getAbsolutePath()+'/';
        GitHelpers.createGitRepo(DocType.TEST);
        GitHelpers.ensureGitRepo(DocType.TEST);
        BDRCReasoner.inferSymetry = true;
//        Model baseModel = TransferHelpers.getOntologyBaseModel();
//        TransferHelpers.bdrcReasoner = BDRCReasoner.getReasoner(baseModel);
        Model ontModel = TransferHelpers.getOntologyModel();
        TransferHelpers.bdrcReasoner = BDRCReasoner.getReasoner(ontModel);
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
	
	public void printDataset(Dataset ds) {
	    System.out.println("printing dataset");
	    Model m = ds.getUnionModel();
	    RDFWriter.create().source(m.getGraph()).context(TransferHelpers.ctx).lang(Lang.TTL).build().output(System.out);
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
	    TransferHelpers.syncTypeFuseki(DocType.TEST, 1000);
	    Model fusekiM = FusekiHelpers.getModel(BDR+"r1");
	    // adding the revision to m so that it corresponds to what's in Fuseki
	    FusekiHelpers.setModelRevision(m, DocType.TEST, rev, "r1");
	    assertTrue(fusekiM.isIsomorphicWith(m));
	    Property p2 = m.createProperty(BDO, "p2");
	    m = ModelFactory.createDefaultModel();
	    m.add(r2, p2, r1);
	    String newRev = writeModelToGitPath(m, "r2.ttl");
	    //System.out.println("just committed "+newRev);
	    assertTrue(GitHelpers.getHeadRev(DocType.TEST).equals(newRev));
        assertTrue(GitHelpers.getLastRefOfFile(DocType.TEST, "r1.ttl").equals(rev));
        TransferHelpers.syncTypeFuseki(DocType.TEST, 1000);
        fusekiM = FusekiHelpers.getModel(BDR+"r2");
        FusekiHelpers.setModelRevision(m, DocType.TEST, newRev, "r2");
        // WHY DOES THIS NOW FAIL??
//        assertTrue(fusekiM.isIsomorphicWith(m));
	}
	
	// BDRC Lib format tests
	@Test
	public void test2() throws IOException {
	    Map<String,Object> res;
	    Map<String,Object> correct;
	    Model person = TransferHelpers.modelFromPath("P1583.ttl", DocType.PERSON, "P1583");
	    res = LibFormat.modelToJsonObject(person, DocType.PERSON);
	    correct = objectFromJson("P1583.json");
	    assertTrue(correct.equals(res));
	    Model work = TransferHelpers.modelFromPath("WorkTestFPL.ttl", DocType.PERSON, "W12837FPL");
        res = LibFormat.modelToJsonObject(work, DocType.WORK);
        correct = objectFromJson("WorkTestFPL.json");
        //System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(res));
        assertTrue(correct.equals(res));
        Model outline = TransferHelpers.modelFromPath("OutlineTest.ttl", DocType.PERSON, "W30020");
        res = LibFormat.modelToJsonObject(outline, DocType.WORK);
        correct = objectFromJson("OutlineTest.json");
        //System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(res));
        assertTrue(correct.equals(res));
        Model itemEtext = TransferHelpers.modelFromPath("ItemEtextTest.ttl", DocType.PERSON, "I21019_E001");
        res = LibFormat.modelToJsonObject(itemEtext, DocType.ITEM);
        correct = objectFromJson("ItemEtextTest.json");
        //System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(res));
        assertTrue(correct.equals(res));
        Model itemImages = TransferHelpers.modelFromPath("ItemImageTest.ttl", DocType.PERSON, "I12827_I001");
        res = LibFormat.modelToJsonObject(itemImages, DocType.ITEM);
        correct = objectFromJson("ItemImageTest.json");
        assertTrue(correct.equals(res));
        Model etext = TransferHelpers.modelFromPath("Etext.ttl", DocType.PERSON, "UT4CZ5369_I1KG9127_0000");
        res = LibFormat.modelToJsonObject(etext, DocType.ETEXT);
        //System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(res));
        correct = objectFromJson("Etext.json");
        assertTrue(correct.equals(res));
	}
	
}
