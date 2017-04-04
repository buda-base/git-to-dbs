package io.brdc.fusekicouchdb;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
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
	private static String placeJsonString = "{\"_id\":\"plc:test0\",\"@graph\":[{\"rdfs:label\":{\"@language\":\"bo\",\"@value\":\"རྒྱ་མཁར་གསང་སྔགས་ཆོས་གླིང\"},\"@id\":\"plc:test0\",\"@type\":[\"plc:DgonPa\"],\"plc:isLocatedIn\":{\"@id\":\"plc:G4250\"}}]}";
	private static String placeRev = null;
	private static List<String> graphNames = new ArrayList<String>();
	
	@BeforeClass
	public static void init() {
		try {
			TransferHelpers.init("localhost", "13180", "localhost", "13598", "test", "testrw");
			TransferHelpers.fu.deleteDefault();
			ObjectMapper om = TransferHelpers.objectMapper;
			ObjectNode placeObject = (ObjectNode)om.readTree(placeJsonString);
			try {
				InputStream oldDocStream = TransferHelpers.db.getAsStream("plc:test0");
				JsonNode oldDoc = om.readTree(oldDocStream);
				placeRev = oldDoc.get("_rev").asText();
				placeObject.put("_rev", placeRev);
				TransferHelpers.db.update(placeObject);
				placeRev = placeObject.get("_rev").asText();
			} catch (DocumentNotFoundException e) {
				TransferHelpers.db.create(placeObject);
				placeRev = placeObject.get("_rev").asText();
			}
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
		ResultSetFormatter.out(System.out, rs, TransferHelpers.pm);
		assertTrue(rs.getRowNumber() == 3);
    }
	
	@Test
    public void test2()
    {
		Model m = ModelFactory.createDefaultModel();
		TransferHelpers.addDocIdInModel("plc:test0", m);
		InfModel im = ModelFactory.createInfModel(TransferHelpers.bdrcReasoner, m);
		Model dm = im.getDeductionsModel();
		//TransferHelpers.printModel(dm);
		Resource test0 = dm.getResource(TransferHelpers.PLACE_PREFIX+"test0");
		Resource place = dm.getResource(TransferHelpers.PLACE_PREFIX+"Place");
		Resource brtenPaGnasKhang = dm.getResource(TransferHelpers.PLACE_PREFIX+"BrtenPaGnasKhang");
		assertTrue(dm.contains(test0, RDF.type, place));
		assertTrue(dm.contains(test0, RDF.type, brtenPaGnasKhang));
    }

}
