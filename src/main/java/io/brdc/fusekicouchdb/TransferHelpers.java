package io.brdc.fusekicouchdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DbAccessException;
import org.ektorp.StreamingViewResult;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult.Row;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

public class TransferHelpers {

	public static final String CORE_PREFIX = "http://onto.bdrc.io/ontologies/bdrc/";
	public static final String DESCRIPTION_PREFIX = "http://onto.bdrc.io/ontology/description#";
	public static final String ROOT_PREFIX = "http://purl.bdrc.io/ontology/root/";
	public static final String COMMON_PREFIX = "http://purl.bdrc.io/ontology/common#";
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
	
	public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
	public static String CouchDBUrl = "http://localhost:13598";
	
	public static CouchDbConnector db = connectCouchDB();
	public static DatasetAccessor fu = connectFuseki();
	
	public static void init(String fusekiHost, String fusekiPort, String couchdbHost, String couchdbPort) {
		FusekiUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/bdrcrw/data";
		CouchDBUrl = "http://" + couchdbHost + ":" +  couchdbPort + "/fuseki/bdrcrw/data";
	}
	
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
	
    private static boolean safeHasNext(Iterator<Row> rowIterator) {
        try {
            return rowIterator.hasNext();
        } catch (DbAccessException e) {
            System.err.println("Failed to move to next row while detecting table");
            return false;
        }
    }
	
	public static List<String> getAllIds() {
		List<String> res = new ArrayList<String>();
		final StreamingViewResult streamingView = db.queryForStreamingView(new ViewQuery().allDocs());
        try {
            final Iterator<Row> rowIterator = streamingView.iterator();
            while (safeHasNext(rowIterator)) {
                Row row = rowIterator.next();
                String Id = row.getId();
                if (!Id.startsWith("_"))
                	res.add(Id);
            }
        } catch (Exception e) {
       		e.printStackTrace();
       	} finally {
       		streamingView.close();
       	}
        return res;
	}
	
	public static void transferCompleteDB () {
		List<String> Ids = getAllIds();
		System.out.println("Transferring " + Ids.size() + " docs to Fuseki");
//		Ids.parallelStream().forEach( (id) -> transferOneDoc(id) );
		
		String id ="start";
		int i = 0;
		for (i = 0; i < Ids.size();) {
			id = Ids.get(i);
			transferOneDoc(id);
			if (++i % 100 == 0) {
				if (i % 1000 == 0) {
					System.out.println(id + ":" + i + ", ");
				} else {
					System.out.print(id + ":" + i + ", ");
				}
			}
		}
		
		System.out.println("\nLast doc transferred: " + id);
		System.out.println("Transferred " + i + " docs to Fuseki");
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
		return jsonLdContext.get(prefix)+finalPart;
	}

	public static void transferOneDoc(String docId) {
		try {
			Model m = ModelFactory.createDefaultModel();
			addDocIdInModel(docId, m);
			//printModel(m);
			String graphName = getFullUrlFromDocId(docId);
			transferModel(graphName, m);
		} catch (Exception ex) {
			System.err.println("Processing: " + docId + " throws " + ex.toString());
		}
	}

	private static void transferModel(String graphName, Model m) {
		fu.add(graphName, m);
	}

	public static void addDocIdInModel(String docId, Model m) {
		// https://github.com/helun/Ektorp/issues/263
		String uri = "/test/_design/jsonld/_show/jsonld/" + docId;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	
	public static OntModel getOntologyModel(String onto)
	{
		OntModel ontoModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM, null);
	    try {
	    	InputStream inputStream;

	    	if (onto != null) {
	    		inputStream = new FileInputStream(onto);
	    	} else {
	    		ClassLoader classLoader = TransferHelpers.class.getClassLoader();
	    		inputStream = classLoader.getResourceAsStream("owl-file/bdrc.owl");
	    	}
	       
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

	public static void transferOntology() {
		OntModel m = getOntologyModel("src/main/resources/bdrc.owl");
		transferModel(ROOT_PREFIX+"baseontology", m);
	}

	public static void transferOntology(String path) {
		OntModel m = getOntologyModel(path);
		transferModel(ROOT_PREFIX+"baseontology", m);
	}
}
