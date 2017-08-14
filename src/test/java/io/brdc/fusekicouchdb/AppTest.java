package io.brdc.fusekicouchdb;

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

/**
 * Unit test for simple App.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AppTest 
{
	private static String placeJsonString = "{\"_id\":\"plc:test0\",\"@graph\":[{\"rdfs:label\":{\"@language\":\"bo\",\"@value\":\"རྒྱ་མཁར་གསང་སྔགས་ཆོས་གླིང\"},\"@id\":\"plc:test0\",\"@type\":[\"plc:DgonPa\"],\"plc:isLocatedIn\":{\"@id\":\"plc:test1\"}}]}";
	private static String personJsonString = "{\"_id\":\"per:testp0\",\"@graph\":[{\"@id\":\"per:testp0\",\"@type\":\"per:Person\",\"per:primaryName\":{\"@language\":\"bo\",\"@value\":\"བློ་བཟང་ཚུལ་ཁྲིམས་བྱམས་པ་རྒྱ་མཚོ\"},\"per:studentOf\":[{\"@id\":\"per:testp1\"}],\"per:teacherOf\":[{\"@id\":\"per:testp2\"}],\"per:gender\":\"male\",\"per:hasOlderBrother\":[{\"@id\":\"per:testp3\"}]}]}";
	private static String placeRev = null;
	private static String personRev = null;
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
			TransferHelpers.fu.deleteDefault();
			ObjectMapper om = TransferHelpers.objectMapper;
			ObjectNode placeObject = (ObjectNode)om.readTree(placeJsonString);
			ObjectNode personObject = (ObjectNode)om.readTree(personJsonString);
			placeRev = overwriteDoc(placeObject, "plc:test0");
			personRev = overwriteDoc(personObject, "per:testp0");
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
		TransferHelpers.transferOntology();
	}
	
	@AfterClass
	public static void finish() {
		TransferHelpers.db.delete("plc:test0", placeRev);
		TransferHelpers.db.delete("per:testp0", personRev);
		TransferHelpers.fu.deleteDefault();
		for (String graphName : graphNames) {
			TransferHelpers.fu.deleteModel(graphName);
		}
		TransferHelpers.executor.shutdown();
	}
	
	@Test
    public void test1()
    {
		String fullId = TransferHelpers.getFullUrlFromDocId("plc:test0");
		//	TransferHelpers.fu.deleteModel(fullId);
		TransferHelpers.transferOneDoc("plc:test0");
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
		TransferHelpers.addDocIdInModel("plc:test0", m);
		InfModel im = TransferHelpers.getInferredModel(m);
		//TransferHelpers.printModel(im);
		Resource test0 = im.getResource(TransferHelpers.PLACE_PREFIX+"test0");
		Resource test1 = im.getResource(TransferHelpers.PLACE_PREFIX+"test1");
		Resource place = im.getResource(TransferHelpers.PLACE_PREFIX+"Place");
		Property contains = im.getProperty(TransferHelpers.PLACE_PREFIX+"contains");
		Resource brtenPaGnasKhang = im.getResource(TransferHelpers.PLACE_PREFIX+"BrtenPaGnasKhang");
		assertTrue(im.contains(test0, RDF.type, place));
		assertTrue(im.contains(test0, RDF.type, brtenPaGnasKhang));
		assertTrue(im.contains(test1, contains, test0));
    }

	@Test
    public void test3()
    {
		Model m = ModelFactory.createDefaultModel();
		TransferHelpers.addDocIdInModel("per:testp0", m);
		InfModel im = TransferHelpers.getInferredModel(m);
		//TransferHelpers.printModel(im);
		Resource testp0 = im.getResource(TransferHelpers.PERSON_PREFIX+"testp0");
		Resource testp1 = im.getResource(TransferHelpers.PERSON_PREFIX+"testp1");
		Resource testp3 = im.getResource(TransferHelpers.PERSON_PREFIX+"testp3");
		Property teacherOf = im.getProperty(TransferHelpers.PERSON_PREFIX+"teacherOf");
		Property hasYoungerBrother = im.getProperty(TransferHelpers.PERSON_PREFIX+"hasYoungerBrother");
		assertTrue(im.contains(testp1, teacherOf, testp0));
		assertTrue(im.contains(testp3, hasYoungerBrother, testp0));
    }
	
}
