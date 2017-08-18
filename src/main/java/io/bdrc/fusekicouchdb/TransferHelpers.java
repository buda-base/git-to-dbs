package io.bdrc.fusekicouchdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.ontology.DatatypeProperty;
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
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
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
	public static final String RESOURCE_PREFIX = "http://purl.bdrc.io/resource/";
	public static final String CORE_PREFIX = "http://purl.bdrc.io/ontology/core/";
	public static final String CONTEXT_URL = "http://purl.bdrc.io/context.jsonld";
	public static final String ADMIN_PREFIX = "http://purl.bdrc.io/ontology/admin/";
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
	
	public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
	public static String CouchDBUrl = "http://localhost:13598";
	public static String FusekiSparqlEndpoint = "http://localhost:13180/fuseki/bdrcrw/query";
	public static String couchdbName = null;
	
	public static CouchDbConnector db = null;
	public static DatasetAccessor fu = null;
	public static OntModel ontModel = null;
	public static Model baseModel = null;
	public static Reasoner bdrcReasoner = null;
	
	public static List<String> database_list = new ArrayList<String>();
	static {
		database_list.add("bdrc_corporation");
		database_list.add("bdrc_lineage");
		database_list.add("bdrc_office");
		database_list.add("bdrc_corporation");
		database_list.add("bdrc_person");
		database_list.add("bdrc_place");
		database_list.add("bdrc_topic");
		database_list.add("bdrc_item");
		database_list.add("bdrc_work");
	}
	
	public static void init(String fusekiHost, String fusekiPort, String couchdbHost, String couchdbPort, String dbName, String fusekiEndpoint) throws MalformedURLException {
		FusekiUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint+"/data";
		FusekiSparqlEndpoint = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint+"/query";
		CouchDBUrl = "http://" + couchdbHost + ":" +  couchdbPort;
		logger.info("connecting to couchdb on "+CouchDBUrl);
		logger.info("connecting to fuseki on "+FusekiUrl);
		fu = connectFuseki();
		baseModel = getOntologyBaseModel(); 
		ontModel = getOntologyModel(baseModel);
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
	
	public static void transferAllDBs(int n) {
		int i = 0;
		for (String dbName : database_list) {
			i =  i + transferCompleteDB(n, dbName);
			if (i >= n)
				return;
		}
		
	}
	
	public static int transferCompleteDB (int n, String dbName) {
		try {
			db =  connectCouchDB(dbName);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return 0;
		}
		couchdbName = dbName;
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
		return i;
	}
	
	public static void transferCompleteDB () {
		transferAllDBs(Integer.MAX_VALUE);
	}
	
	public static DatasetAccessor connectFuseki() {
		return DatasetAccessorFactory.createHTTP(FusekiUrl);
	}
	
	public static String getFullUrlFromDocId(String docId) {
		int colonIndex = docId.indexOf(":");
		if (colonIndex < 0) return docId;
		return RESOURCE_PREFIX+docId.substring(colonIndex+1);
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
	    StreamRDF dest = StreamRDFLib.graph(m.getGraph());
	    RDFParserBuilder rdfp = RDFParser.source(stuff).lang(Lang.JSONLD);
	    rdfp.parse(dest);
	    try {
			stuff.close();
		} catch (IOException e) {
			logger.error("This really shouldn't happen!", e);
		}
	}
	
	public static void updateFusekiLastSequence(String sequence) {
		Model m = getSyncModel();
		Resource res = m.getResource(ADMIN_PREFIX+"CouchdbSyncInfo");
		Property p = m.getProperty(ADMIN_PREFIX+"has_last_sequence");
		Literal l = m.createLiteral(sequence);
		Statement s = m.getProperty(res, p);
		if (s == null) {
			m.add(res, p, l);
		} else {
			s.changeObject(sequence);
		}
		try {
			transferModel(ADMIN_PREFIX+"system", m);
		} catch (TimeoutException e) {
			logger.warn("Timeout sending sequence to fuseki (not fatal): "+sequence, e);
		}
	}
	
	private static Model syncModel = null;
	
	public static synchronized Model getSyncModel() {
		if (syncModel != null) return syncModel;
		try {
			syncModel = getModel(ADMIN_PREFIX+"system");
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
		Resource res = m.getResource(ADMIN_PREFIX+"CouchdbSyncInfo");
		Property p = m.getProperty(ADMIN_PREFIX+"has_last_sequence");
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
		//doc.put("@context", jsonLdContext);
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
	
	public static ResultSet selectSparql(String query) {
		QueryExecution qe = QueryExecutionFactory.sparqlService(FusekiSparqlEndpoint, query);
		return qe.execSelect();
	}
	
	public static Model getOntologyBaseModel() {
		Model res;
		try {
    		ClassLoader classLoader = TransferHelpers.class.getClassLoader();
    		InputStream inputStream = classLoader.getResourceAsStream("owl-schema/bdrc.owl");
	        res = ModelFactory.createDefaultModel();
	    	res.read(inputStream, "", "RDF/XML");
	        inputStream.close();
	    } catch (Exception e) {
	    	logger.error("Error reading ontology file", e);
	    	return null;
	    }
		return res;
	}
	
	public static OntModel getOntologyModel(Model baseModel) {
		OntModel ontoModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM, baseModel);
	    rdf10tordf11(ontoModel);
	    return ontoModel;
	}

	public static void transferOntology() {
		try {
			transferModel(CORE_PREFIX+"ontologySchema", ontModel);
		} catch (TimeoutException e) {
			logger.error("Timeout sending ontology model", e);
		}
	}
}
