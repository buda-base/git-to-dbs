package io.brdc.fusekicouchdb;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.Derivation;
import org.ektorp.DocumentNotFoundException;
import org.junit.After;
import org.junit.Before;
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
	private String placeJsonString = "{\"_id\":\"plc:test0\",\"@graph\":[{\"rdfs:label\":{\"@language\":\"bo\",\"@value\":\"རྒྱ་མཁར་གསང་སྔགས་ཆོས་གླིང\"},\"@id\":\"plc:test0\",\"@type\":[\"plc:Place\",\"plc:DgonPa\"],\"plc:isLocatedIn\":{\"@id\":\"plc:G4250\"}}]}";
	private String placeRev = null;
	private List<String> graphNames = new ArrayList<String>();
	
	@Before
	public void init() {
		try {
			TransferHelpers.init("localhost", "13180", "localhost", "13598", "test", "testrw");
			//TransferHelpers.fu.deleteDefault();
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
	
	@After
	public void finish() {
//		TransferHelpers.db.delete("plc:test0", placeRev);
//		TransferHelpers.fu.deleteDefault();
//		for (String graphName : graphNames) {
//			TransferHelpers.fu.deleteModel(graphName);
//		}
		TransferHelpers.executor.shutdown();
	}
	
//	@Test
//    public void test1()
//    {
//		String fullId = TransferHelpers.getFullUrlFromDocId("plc:test0");
//		//	TransferHelpers.fu.deleteModel(fullId);
//		TransferHelpers.transferOneDoc("plc:test0");
//		graphNames.add(fullId);
//		String query = "SELECT ?p ?l "
//				+ "WHERE {  <"+fullId+"> ?p ?l }";
//		ResultSet rs = TransferHelpers.selectSparql(query);
//		ResultSetFormatter.out(System.out, rs, TransferHelpers.pm);
//		assertTrue(rs.getRowNumber() == 4);
//    }
	
	@Test
    public void test2()
    {
		Model m = ModelFactory.createDefaultModel();
		TransferHelpers.addDocIdInModel("plc:test0", m);
		InfModel im = ModelFactory.createInfModel(TransferHelpers.bdrcReasoner, m);
		//im.setDerivationLogging(true);
		im.prepare();
		Model dm = im.getDeductionsModel();
		System.out.println("deduced triples:");
		TransferHelpers.printModel(dm);
		// shows all deducted rules for the whole ontology (not very interesting)
//		PrintWriter out = new PrintWriter(System.out);
//		for (StmtIterator i = im.listStatements(); i.hasNext(); ) {
//		    Statement s = i.nextStatement();
//		    System.out.println("Statement is " + s);
//		    for (Iterator<Derivation> id = im.getDerivation(s); id.hasNext(); ) {
//		        Derivation deriv = id.next();
//		        deriv.printTrace(out, true);
//		    }
//		}
//		out.flush();
    }

}
