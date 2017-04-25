package io.brdc.fusekicouchdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.ontology.DatatypeProperty;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.ontology.Restriction;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.rulesys.RDFSRuleReasonerFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.ReasonerVocabulary;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TransferHelpers {
	public static final String DESCRIPTION_PREFIX = "http://onto.bdrc.io/ontology/description#";
	public static final String ROOT_PREFIX = "http://purl.bdrc.io/ontology/root#";
	public static final String CORPORATION_PREFIX = "http://purl.bdrc.io/ontology/corporation#";
	public static final String LINEAGE_PREFIX = "http://purl.bdrc.io/ontology/lineage#";
	public static final String OFFICE_PREFIX = "http://purl.bdrc.io/ontology/office#";
	public static final String PRODUCT_PREFIX = "http://purl.bdrc.io/ontology/product#";
	public static final String OUTLINE_PREFIX = "http://purl.bdrc.io/ontology/outline#";
	public static final String PERSON_PREFIX = "http://purl.bdrc.io/ontology/person#";
	public static final String PLACE_PREFIX = "http://purl.bdrc.io/ontology/place#";
	public static final String TOPIC_PREFIX = "http://purl.bdrc.io/ontology/topic#";
	public static final String VOLUMES_PREFIX = "http://purl.bdrc.io/ontology/volumes#";
	public static final String WORK_PREFIX = "http://purl.bdrc.io/ontology/work#";
	public static final String OWL_PREFIX = "http://www.w3.org/2002/07/owl#";
	public static final String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static final String RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#";
	public static final String XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";
	
	public static Logger logger = LoggerFactory.getLogger("fuseki-couchdb");
	
	public static boolean progress = false;
	
	public static long TRANSFER_TO = 15; // seconds
	
	public static ExecutorService executor = Executors.newCachedThreadPool();
	
	public static ObjectMapper objectMapper = new ObjectMapper();
	public static ObjectNode jsonLdContext = objectMapper.createObjectNode();
	
	public static PrefixMapping pm = null;
	
	static {
		jsonLdContext.put("crp", CORPORATION_PREFIX);
		jsonLdContext.put("prd", PRODUCT_PREFIX);
		jsonLdContext.put("owl", OWL_PREFIX);
		jsonLdContext.put("plc", PLACE_PREFIX);
		jsonLdContext.put("xsd", XSD_PREFIX);
		jsonLdContext.put("rdfs", RDFS_PREFIX);
		jsonLdContext.put("rdf", RDF_PREFIX);
		jsonLdContext.put("ofc", OFFICE_PREFIX);
		jsonLdContext.put("out", OUTLINE_PREFIX);
		jsonLdContext.put("lin", LINEAGE_PREFIX);
		jsonLdContext.put("top", TOPIC_PREFIX);
		jsonLdContext.put("wor", WORK_PREFIX);
		jsonLdContext.put("per", PERSON_PREFIX);
		jsonLdContext.put("vol", VOLUMES_PREFIX);
		jsonLdContext.put("desc", DESCRIPTION_PREFIX);
		jsonLdContext.put("", ROOT_PREFIX);
		jsonLdContext.put("@vocab", ROOT_PREFIX);
	}
	
	public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
	public static String CouchDBUrl = "http://localhost:13598";
	public static String FusekiSparqlEndpoint = "http://localhost:13180/fuseki/bdrcrw/query";
	public static String couchdbName = null;
	
	public static CouchDbConnector db = null;
	public static DatasetAccessor fu = null;
	public static OntModel ontModel = null;
	public static Reasoner bdrcReasoner = null;
	
	public static void init(String fusekiHost, String fusekiPort, String couchdbHost, String couchdbPort, String dbName, String fusekiEndpoint) throws MalformedURLException {
		FusekiUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint+"/data";
		FusekiSparqlEndpoint = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint+"/query";
		CouchDBUrl = "http://" + couchdbHost + ":" +  couchdbPort;
		logger.info("connecting to couchdb on "+CouchDBUrl);
		db = connectCouchDB(dbName);
		logger.info("connecting to fuseki on "+FusekiUrl);
		fu = connectFuseki();
		couchdbName = dbName;
		pm = PrefixMapping.Factory.create();
		Iterator<Entry<String, JsonNode>> nodes = jsonLdContext.fields();
		while (nodes.hasNext()) {
		  Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) nodes.next();
		  String key = entry.getKey();
		  if (key == "@vocab") continue;
		  pm.setNsPrefix(key, entry.getValue().asText());
		}
		ontModel = getOntologyModel(null);
		bdrcReasoner = BDRCReasoner.getReasoner(ontModel);
	}
	
	public static CouchDbConnector connectCouchDB(String couchdbName) throws MalformedURLException {
		HttpClient httpClient;
		CouchDbInstance dbInstance;
		CouchDbConnector res = null;
        httpClient = new StdHttpClient.Builder()
                .url(CouchDBUrl)
                .build();
        dbInstance = new StdCouchDbInstance(httpClient);
        res = new StdCouchDbConnector(couchdbName, dbInstance);
    	return res;
	}
	
	public static void transferCompleteDB (int n) {
		List<String> Ids = db.getAllDocIds();
		int lim = Integer.min(Ids.size(), n);
		String lastSequence = (lim < Ids.size()) ? "0" : getLastCouchDBChangeSeq();
		logger.info("Transferring " + lim + " docs to Fuseki");
//		Ids.parallelStream().forEach( (id) -> transferOneDoc(id) );
		
		String id ="start";
		int i = 0;
		for (i = 0; i < lim; i++) {
			id = Ids.get(i);
			if (id.startsWith("_design")) continue;
			transferOneDoc(id);
			if (i % 100 == 0 && progress) {
				logger.info(id + ":" + i);
			}
		}
		logger.info("Last doc transferred: " + id);
		logger.info("Transferred " + i + " docs to Fuseki");
		updateFusekiLastSequence(lastSequence);
		logger.info("Updating sequence to "+lastSequence);
	}
	
	public static void transferCompleteDB () {
		transferCompleteDB(Integer.MAX_VALUE);
	}
	
	public static DatasetAccessor connectFuseki() {
		return DatasetAccessorFactory.createHTTP(FusekiUrl);
	}
	
	public static String getFullUrlFromDocId(String docId) {
		int colonIndex = docId.indexOf(":");
		if (colonIndex < 0) return docId;
		String prefix = docId.substring(0 , colonIndex);
		String finalPart = docId.substring(colonIndex+1);
		if (prefix.isEmpty()) prefix = "@vocab";
		String longPrefix = jsonLdContext.get(prefix).asText();
		return longPrefix+finalPart;
	}

	public static void transferOneDoc(String docId) {
		try {
			Model m = ModelFactory.createDefaultModel();
			addDocIdInModel(docId, m);
			m = getInferredModel(m);
			//printModel(m);
			String graphName = getFullUrlFromDocId(docId);
			transferModel(graphName, m);
			m.close();
		} catch (Exception ex) {
			logger.error("Error transfering "+docId, ex);
		}
	}
	
	private static void transferModel(String graphName, Model m) throws TimeoutException {
		callFuseki("putModel", graphName, m);
	}
	
	private static void deleteModel(String graphName) throws TimeoutException {
		callFuseki("deleteModel", graphName, null);
	}
	
	private static Model getModel(String graphName) throws TimeoutException {
		return callFuseki("getModel", graphName, null);
	}
	
	private static Model callFuseki(String operation, String graphName, Model m) throws TimeoutException {
		Model res = null;
		Callable<Model> task = new Callable<Model>() {
		   public Model call() throws InterruptedException {
			  switch (operation) {
			  case "putModel":
				  fu.putModel(graphName, m);
				  //fu.putModel(m);
				  return null;
			  case "deleteModel":
				  fu.deleteModel(graphName);
				  return null;
			  default:
				  return fu.getModel(graphName);
			  }
		   }
		};
		Future<Model> future = executor.submit(task);
		try {
		   res = future.get(TRANSFER_TO, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("interrupted during "+operation+" of "+graphName, e);
		} catch (ExecutionException e) {
		   logger.error("execution error during "+operation+" of "+graphName+", this shouldn't happen, quitting...", e);
		   System.exit(1);
		} finally {
		   future.cancel(true); // this kills the transfer
		}
		return res;
	}

	public static InfModel getInferredModel(Model m) {
		return ModelFactory.createInfModel(TransferHelpers.bdrcReasoner, m);
	}
	
	public static void addDocIdInModel(String docId, Model m) {
		// https://github.com/helun/Ektorp/issues/263
		String uri = "/"+couchdbName+"/_design/jsonld/_show/jsonld/" + docId;
	    HttpResponse r = db.getConnection().get(uri);
	    InputStream stuff = r.getContent();
	    // feeding the inputstream directly to jena for json-ld parsing
	    // instead of ektorp json parsing
	    StreamRDF dest = StreamRDFLib.graph(m.getGraph()) ;
	    Context ctx = new Context();
	    RDFDataMgr.parse(dest, stuff, "", Lang.JSONLD, ctx);
	    try {
			stuff.close();
		} catch (IOException e) {
			logger.error("This really shouldn't happen!", e);
		}
	}
	
	public static void updateFusekiLastSequence(String sequence) {
		Model m = getSyncModel();
		Resource res = m.getResource(ROOT_PREFIX+"CouchdbSyncInfo");
		Property p = m.getProperty(ROOT_PREFIX+"has_last_sequence");
		Literal l = m.createLiteral(sequence);
		Statement s = m.getProperty(res, p);
		if (s == null) {
			m.add(res, p, l);
		} else {
			s.changeObject(sequence);
		}
		try {
			transferModel(ROOT_PREFIX+"system", m);
		} catch (TimeoutException e) {
			logger.warn("Timeout sending sequence to fuseki (not fatal): "+sequence, e);
		}
	}
	
	private static Model syncModel = null;
	
	public static synchronized Model getSyncModel() {
		if (syncModel != null) return syncModel;
		try {
			syncModel = getModel(ROOT_PREFIX+"system");
		} catch (TimeoutException e) {
			logger.error("Time out while fetching system model, quitting...", e);
			System.exit(1);
		}
		if (syncModel == null) {
			syncModel = ModelFactory.createDefaultModel();
		}
		return syncModel;
	}
	
	public static String getLastFusekiSequence() { 
		Model m = getSyncModel();
		Resource res = m.getResource(ROOT_PREFIX+"CouchdbSyncInfo");
		Property p = m.getProperty(ROOT_PREFIX+"has_last_sequence");
		Statement s = m.getProperty(res, p);
		if (s == null) return null;
		return s.getString();
	}
	
	public static String getLastCouchDBChangeSeq() {
		ChangesCommand cmd = new ChangesCommand.Builder()
				.param("descending", "true")
				.limit(1)
				.build();
		ChangesFeed feed = db.changesFeed(cmd);
		while (feed.isAlive()) {
			try {
				DocumentChange change = feed.next();
				return change.getStringSequence();
			} catch (InterruptedException e) {
				logger.warn("Interruption while fetching couchDB changes", e);
			}
		}
		return null;
	}
	
	public static Model couchDocToModel(ObjectNode doc) {
		Model m = ModelFactory.createDefaultModel();
		doc.remove("_id");
		doc.remove("_rev");
		doc.put("@context", jsonLdContext);
		// quite inefficient: we have to convert the doc to string
		// to feed it to jena json-ld parser...
		String docstring;
		try {
			docstring = objectMapper.writeValueAsString(doc);
		} catch (JsonProcessingException e) {
			logger.error("This really shouldn't happen!", e);
			return null;
		}
		StringReader docreader = new StringReader(docstring);
		m.read(docreader, "", "JSON-LD");
		return m;
	}
	
	public static void transferChange(DocumentChange change) {
		String id = change.getId();
		if (change.getId().startsWith("_design")) {
			return;
		}
		String fullId = getFullUrlFromDocId(id);
		String sequence = change.getStringSequence();
		if (change.isDeleted()) {
			logger.info("deleting " + fullId);
			try {
				deleteModel(fullId);
			} catch (TimeoutException e) {
				logger.error("Timeout deleting model: "+fullId, e);
			}
			updateFusekiLastSequence(sequence);
			return;
		}
		logger.info("updating " + fullId);
		JsonNode o = change.getDocAsNode();
		Model m = couchDocToModel((ObjectNode) o);
		o = null; change = null;//gc?
		try {
			transferModel(fullId, m);
			m.close();
		} catch (TimeoutException e) {
			logger.error("Timeout sending model: "+fullId, e);
		}
		updateFusekiLastSequence(sequence);
		logger.info("last sequence set to "+sequence);
	}
	
	// for debugging purposes only
	public static void printModel(Model m) {
		RDFDataMgr.write(System.out, m, RDFFormat.TURTLE_PRETTY);
	}

	// change Range Datatypes from rdf:PlainLitteral to rdf:langString
	public static void rdf10tordf11(OntModel o) {
		Resource RDFPL = o.getResource("http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral");
		Resource RDFLS = o.getResource("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");
		ExtendedIterator<DatatypeProperty> it = o.listDatatypeProperties();
	    while(it.hasNext()) {
			DatatypeProperty p = it.next();
			if (p.hasRange(RDFPL)) {
			    p.removeRange(RDFPL);
			    p.addRange(RDFLS);
			}
	    }
	    ExtendedIterator<Restriction> it2 = o.listRestrictions();
	    while(it2.hasNext()) {
            Restriction r = it2.next();
            Statement s = r.getProperty(OWL2.onDataRange); // is that code obvious? no
            if (s != null && s.getObject().asResource().equals(RDFPL)) {
                s.changeObject(RDFLS);

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
	
	public static ResultSet selectSparql(String query) {
		QueryExecution qe = QueryExecutionFactory.sparqlService(FusekiSparqlEndpoint, query);
		return qe.execSelect();
	}
	
	public static OntModel getOntologyModel(String onto)
	{
		OntModel ontoModel;;
	    try {
	    	InputStream inputStream;

	    	if (onto != null) {
	    		inputStream = new FileInputStream(onto);
	    	} else {
	    		ClassLoader classLoader = TransferHelpers.class.getClassLoader();
	    		inputStream = classLoader.getResourceAsStream("owl-schema/bdrc.owl");
	    	}
	        Model m = ModelFactory.createDefaultModel();
	    	m.read(inputStream, "", "RDF/XML");
	        inputStream.close();
	        // if you want inferred triples:
	        //Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
	        //InfModel infModel = ModelFactory.createInfModel(reasoner, m);
	        ontoModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM, m);
	    } catch (Exception e) {
	    	logger.error("Error reading ontology file", e);
	    	return null;
	    }
	    // then we fix it by removing the individuals and converting rdf10 to rdf11
	    removeIndividuals(ontoModel);
	    rdf10tordf11(ontoModel);
	    return ontoModel;
	}

	public static void transferOntology() {
		try {
			transferModel(ROOT_PREFIX+"baseontology", ontModel);
		} catch (TimeoutException e) {
			logger.error("Timeout sending ontology model", e);
		}
	}
}
