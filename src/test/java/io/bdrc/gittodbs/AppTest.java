package io.bdrc.gittodbs;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.ektorp.DocumentNotFoundException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.bdrc.gittodbs.BDRCReasoner;
import io.bdrc.gittodbs.TransferHelpers;

/**
 * Unit test for simple App.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AppTest 
{
	private static String placeJsonString = "{\"_id\":\"bdr:test0\",\"@graph\":[{\"rdfs:label\":{\"@language\":\"bo\",\"@value\":\"རྒྱ་མཁར་གསང་སྔགས་ཆོས་གླིང\"},\"@id\":\"bdr:test0\",\"@type\":\"bdo:Place\",\"placeLocatedIn\":\"bdr:test1\"}],\"@context\":\""+TransferHelpers.CONTEXT_URL+"\"}";
	private static String personJsonString = "{\"_id\":\"bdr:testp0\",\"@graph\":[{\"@id\":\"bdr:testp0\",\"@type\":\"bdo:Person\",\"personStudentOf\":\"bdr:testp1\",\"personTeacherOf\":\"bdr:testp2\"}],\"@context\":\""+TransferHelpers.CONTEXT_URL+"\"}";
	private static String placeRev = null;
	private static String personRev = null;
	private static final String BDR = TransferHelpers.RESOURCE_PREFIX;
	private static final String BDO = TransferHelpers.CORE_PREFIX;
	private static List<String> graphNames = new ArrayList<String>();
	
	public static String overwriteDoc(ObjectNode object, String id) throws JsonProcessingException, IOException {
		String res;
		try {
			InputStream oldDocStream = TransferHelpers.db.getAsStream(id);
			JsonNode oldDoc = TransferHelpers.objectMapper.readTree(oldDocStream);
			res = oldDoc.get("_rev").asText();
			object.put("_rev", res);
			TransferHelpers.db.update(object);
			res = object.get("_rev").asText();
		} catch (DocumentNotFoundException e) {
			TransferHelpers.db.create(object);
			res = object.get("_rev").asText();
		}
		return res;
	}
	
	@BeforeClass
	public static void init() {
		try {
			TransferHelpers.init("localhost", "13180", "localhost", "13598", "test", "testrw");
			try {
				TransferHelpers.db =  TransferHelpers.connectCouchDB("test");
			} catch (MalformedURLException e) {
				e.printStackTrace();
				return;
			}
			TransferHelpers.couchdbName = "test";
			TransferHelpers.fu.deleteDefault();
			ObjectMapper om = TransferHelpers.objectMapper;
			ObjectNode placeObject = (ObjectNode)om.readTree(placeJsonString);
			ObjectNode personObject = (ObjectNode)om.readTree(personJsonString);
			placeRev = overwriteDoc(placeObject, "bdr:test0");
			personRev = overwriteDoc(personObject, "bdr:testp0");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		//TransferHelpers.transferOntology();
	}
	
	@AfterClass
	public static void finish() {
		TransferHelpers.db.delete("bdr:test0", placeRev);
		TransferHelpers.db.delete("bdr:testp0", personRev);
		TransferHelpers.fu.deleteDefault();
		for (String graphName : graphNames) {
			TransferHelpers.fu.deleteModel(graphName);
		}
		TransferHelpers.executor.shutdown();
	}
	
	@Test
    public void test1()
    {
		String fullId = TransferHelpers.getFullUrlFromDocId("bdr:test0");
		//	TransferHelpers.fu.deleteModel(fullId);
		TransferHelpers.transferOneDoc("bdr:test0");
		graphNames.add(fullId);
		String query = "SELECT ?p ?l "
				+ "WHERE {  <"+fullId+"> ?p ?l }";
		ResultSet rs = TransferHelpers.selectSparql(query);
		assertTrue(rs.hasNext());
		//ResultSetFormatter.out(System.out, rs, TransferHelpers.pm);
    }
	
	@Test
    public void test2()
    {
		Model m = ModelFactory.createDefaultModel();
		TransferHelpers.addDocIdInModel("bdr:test0", m);
		InfModel im = TransferHelpers.getInferredModel(m);
		//TransferHelpers.printModel(im);
		Resource test0 = im.getResource(BDR+"test0");
		Resource test1 = im.getResource(BDR+"test1");
		Resource place = im.getResource(BDO+"Place");
		Property contains = im.getProperty(BDO+"placeContains");
		assertTrue(im.contains(test0, RDF.type, place));
		if (BDRCReasoner.inferSymetry) {
			assertTrue(im.contains(test1, contains, test0));			
		}
    }

	@Test
    public void test3()
    {
		Model m = ModelFactory.createDefaultModel();
		TransferHelpers.addDocIdInModel("bdr:testp0", m);
		InfModel im = TransferHelpers.getInferredModel(m);
		//TransferHelpers.printModel(im);
		Resource testp0 = im.getResource(BDR+"testp0");
		Resource testp1 = im.getResource(BDR+"testp1");
		Property teacherOf = im.getProperty(BDR+"personTeacherOf");
		if (BDRCReasoner.inferSymetry) {
			assertTrue(im.contains(testp1, teacherOf, testp0));			
		}
    }

	@Test
    public void test4()
    {
		Model m = ModelFactory.createDefaultModel();
		TransferHelpers.addDocIdInModel("bdr:test0", m);
		m.add(m.createResource(BDR+"test0"), 
				m.createProperty(BDO, "stupidProp"), 
				m.createResource(BDR+"TraditionTaklungKagyu"));
		InfModel im = TransferHelpers.getInferredModel(m);
		//TransferHelpers.printModel(im);
		assertTrue(im.contains(im.createResource(BDR+"test0"), 
				im.createProperty(BDO, "stupidProp"), 
				im.createResource(BDR+"TraditionKagyu")));
    }
	
}
