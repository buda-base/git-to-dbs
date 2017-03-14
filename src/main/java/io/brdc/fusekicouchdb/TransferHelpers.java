package io.brdc.fusekicouchdb;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jena.ontology.DatatypeProperty;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RIOT;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransferHelpers {

	public static final String CORE_PREFIX = "http://onto.bdrc.io/ontologies/bdrc/";
	public static final String DESCRIPTION_PREFIX = "http://onto.bdrc.io/ontology/description#";
	public static final String ROOT_PREFIX = "http://purl.bdrc.io/ontology/root/";
	public static final String COMMON_PREFIX = "http://purl.bdrc.io/ontology/common#";
	public static final String CORPORATION_PREFIX = "http://purl.bdrc.io/ontology/coroporation#";
	public static final String LINEAGE_PREFIX = "http://purl.bdrc.io/ontology/lineage#";
	public static final String OFFICE_PREFIX = "http://purl.bdrc.io/ontology/office#";
	public static final String PRODUCT_PREFIX = "http://purl.bdrc.io/ontology/product#";
	public static final String OUTLINE_PREFIX = "http://purl.bdrc.io/ontology/outline#";
	public static final String PERSON_PREFIX = "http://purl.bdrc.io/ontology/person#";
	public static final String PLACE_PREFIX = "http://purl.bdrc.io/ontology/place#";
	public static final String TOPIC_PREFIX = "http://purl.bdrc.io/ontology/topic#";
	public static final String VOLUMES_PREFIX = "http://purl.bdrc.io/ontology/volumes#";
	public static final String WORK_PREFIX = "http://purl.bdrc.io/ontology/work/";
	public static final String OWL_PREFIX = "http://www.w3.org/2002/07/owl#";
	public static final String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static final String RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#";
	public static final String XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";
	
	public static void setPrefixes(Model m) {
		m.setNsPrefix("com", COMMON_PREFIX);
		m.setNsPrefix("", ROOT_PREFIX);
		m.setNsPrefix("per", PERSON_PREFIX);
		m.setNsPrefix("prd", PRODUCT_PREFIX);
		m.setNsPrefix("wor", WORK_PREFIX);
		m.setNsPrefix("out", OUTLINE_PREFIX);
		m.setNsPrefix("plc", PLACE_PREFIX);
		m.setNsPrefix("top", TOPIC_PREFIX);
		m.setNsPrefix("lin", LINEAGE_PREFIX);
		m.setNsPrefix("vol", VOLUMES_PREFIX);
		m.setNsPrefix("crp", CORPORATION_PREFIX);
		m.setNsPrefix("ofc", OFFICE_PREFIX);
		m.setNsPrefix("owl", OWL_PREFIX);
		m.setNsPrefix("rdf", RDF_PREFIX);
		m.setNsPrefix("rdfs", RDFS_PREFIX);
		m.setNsPrefix("xsd", XSD_PREFIX);
		m.setNsPrefix("desc", DESCRIPTION_PREFIX);
	}
	
	public static Map<String,String> jsonLdContext = new LinkedHashMap<String, String>();
	static {
		jsonLdContext.put("@vocab", ROOT_PREFIX);
		jsonLdContext.put("com", COMMON_PREFIX);
		jsonLdContext.put("crp", CORPORATION_PREFIX);
		jsonLdContext.put("prd", PRODUCT_PREFIX);
		jsonLdContext.put("owl", OWL_PREFIX);
		jsonLdContext.put("plc", PLACE_PREFIX);
		jsonLdContext.put("xsd", XSD_PREFIX);
		jsonLdContext.put("rdfs", RDFS_PREFIX);
		jsonLdContext.put("ofc", OFFICE_PREFIX);
		jsonLdContext.put("out", OUTLINE_PREFIX);
		jsonLdContext.put("lin", LINEAGE_PREFIX);
		jsonLdContext.put("top", TOPIC_PREFIX);
		jsonLdContext.put("wor", WORK_PREFIX);
		jsonLdContext.put("per", PERSON_PREFIX);
		jsonLdContext.put("vol", VOLUMES_PREFIX);
		jsonLdContext.put("desc", DESCRIPTION_PREFIX);
	}
	
	public static final String FusekiUrl = "http://localhost:13180/fuseki/test/data";
	public static final String CouchDBUrl = "http://localhost:5984";
	
	public static CouchDbConnector db = connectCouchDB();
	public static DatasetAccessor fu = connectFuseki();
	
	public static CouchDbConnector connectCouchDB() {
		HttpClient httpClient;
		CouchDbInstance dbInstance;
		CouchDbConnector res = null;
    	try {
            httpClient = new StdHttpClient.Builder()
                    .url(CouchDBUrl)
                    .build();
            dbInstance = new StdCouchDbInstance(httpClient);
            // http://localhost:5984/test/_design/jsonld/_show/jsonld/
            res = new StdCouchDbConnector("test", dbInstance);
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    	return res;
	}
	
	public static DatasetAccessor connectFuseki() {
		return DatasetAccessorFactory.createHTTP(FusekiUrl);
	}
	
	public static void transferOneDoc(String docId) {
		Model m = ModelFactory.createDefaultModel();
		setPrefixes(m);
		addDocIdInModel(docId, m);
		printModel(m);
		transferModel(m);
	}
	
	private static void transferModel(Model m) {
		fu.add(m);
	}

	public static void addDocIdInModel(String docId, Model m) {
		// this part is really frustrating: we have to convert
		// to object (through db.get) then remove the _id and _rev
		// fields to convert it back to string to give it to jena, which will
		// in term convert it back to object. If we don't do that, jsonld-java
		// gets confused by these fields and does not parse the file correctly.
		// A solution would have been to use a show couchdb function, but
		// https://github.com/helun/Ektorp/issues/263
		Map<String,Object> doc = db.get(Map.class, docId);
		doc.remove("_id");
		doc.remove("_rev");
		doc.put("@context", jsonLdContext);
		ObjectMapper mapper = new ObjectMapper();
		String docstring;
		try {
			docstring = mapper.writeValueAsString(doc);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		System.out.println(docstring);
		StringReader docreader = new StringReader(docstring);
		m.read(docreader, "", "JSON-LD");
		//StreamRDF dest = StreamRDFLib.graph(m.getGraph()) ;
		//RDFDataMgr.parse(dest, docreader, "", Lang.JSONLD, ctx);
	}
	
	// for debugging purposes only
	public static void printModel(Model m) {
		RDFDataMgr.write(System.out, m, RDFFormat.TURTLE_PRETTY);
	}

	// change Range Datatypes from rdf:PlainLitteral to rdf:langString
	// Warning: only works for 
	public static void rdf10tordf11(OntModel o) {
		Resource RDFPL = o.getResource("http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral");
		Resource RDFLS = o.getResource("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");
		ExtendedIterator<DatatypeProperty> it = o.listDatatypeProperties();
	    while(it.hasNext()) {
			DatatypeProperty p = it.next();
			Resource r = p.getRange();
			if (r != null && r.equals(RDFPL)) {
				p.setRange(RDFLS);
			}
	    }
	}
	
	public static void removeIndividuals(OntModel o) {
		ExtendedIterator<Individual> it = o.listIndividuals();
	    while(it.hasNext()) {
			Individual i = it.next();
			if (i.getLocalName().equals("UNKNOWN")) continue;
			i.remove();
	    }
	}
	
	public static OntModel getOntologyModel()
	{
		OntModel ontoModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM, null);
	    try {
	        InputStream inputStream = new FileInputStream("src/main/resources/owl/bdrc.owl");
	        ontoModel.read(inputStream, "", "RDF/XML");
	        inputStream.close();
	    } catch (Exception e) {
	        System.err.println(e.getMessage());
	    }
	    // then we fix it by removing the individuals and converting rdf10 to rdf11
	    removeIndividuals(ontoModel);
	    rdf10tordf11(ontoModel);
	    return ontoModel;
	}
	
	
}
