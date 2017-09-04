package io.bdrc.gittodbs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.jena.ontology.DatatypeProperty;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.ontology.Restriction;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferHelpers {
	public static final String RESOURCE_PREFIX = "http://purl.bdrc.io/resource/";
	public static final String CORE_PREFIX = "http://purl.bdrc.io/ontology/core/";
	public static final String CONTEXT_URL = "http://purl.bdrc.io/context.jsonld";
	public static final String ADMIN_PREFIX = "http://purl.bdrc.io/ontology/admin/";
	public static final String DATA_PREFIX = "http://purl.bdrc.io/data/";
    public static final String SKOS_PREFIX = "http://www.w3.org/2004/02/skos/core#";
    public static final String VCARD_PREFIX = "http://www.w3.org/2006/vcard/ns#";
    public static final String TBR_PREFIX = "http://purl.bdrc.io/ontology/toberemoved/";
    public static final String OWL_PREFIX = "http://www.w3.org/2002/07/owl#";
    public static final String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#";
    public static final String XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";
    
    public static final String BDR = RESOURCE_PREFIX;
    public static final String BDO = CORE_PREFIX;
	
	public enum DocType {
	    CORPORATION,
	    LINEAGE,
	    OFFICE,
	    PERSON,
	    PLACE,
	    TOPIC,
	    ITEM,
	    WORK,
	    PRODUCT
	    ;  
	  }
	
    public static PrefixMap getPrefixMap() {
        PrefixMap pm = PrefixMapFactory.create();
        pm.add("", CORE_PREFIX);
        pm.add("adm", ADMIN_PREFIX);
        pm.add("bdd", DATA_PREFIX);
        pm.add("bdr", RESOURCE_PREFIX);
        pm.add("tbr", TBR_PREFIX);
        pm.add("owl", OWL_PREFIX);
        pm.add("rdf", RDF_PREFIX);
        pm.add("rdfs", RDFS_PREFIX);
        pm.add("skos", SKOS_PREFIX);
        pm.add("vcard", VCARD_PREFIX);
        pm.add("xsd", XSD_PREFIX);
        return pm;
    }
	
	public static Logger logger = LoggerFactory.getLogger("git2dbs");
	
	public static boolean progress = false;
	
	public static long TRANSFER_TO = 15; // seconds
	
	public static ExecutorService executor = Executors.newCachedThreadPool();


	public static OntModel ontModel = null;
	public static Model baseModel = null;
	public static Reasoner bdrcReasoner = null;
	
	public static void init() throws MalformedURLException {
	    if (GitToDB.transferFuseki) {
	        FusekiHelpers.init(GitToDB.fusekiHost, GitToDB.fusekiPort, GitToDB.fusekiName);	        
	    }
	    if (GitToDB.transferCouch) {
	        CouchHelpers.init(GitToDB.couchdbHost, GitToDB.couchdbPort);
	    }
		baseModel = getOntologyBaseModel(); 
		ontModel = getOntologyModel(baseModel);
		bdrcReasoner = BDRCReasoner.getReasoner(ontModel);
	}
	
	public static void sync(int howMany) {
	    
	}
	
	public static String getFullUrlFromDocId(String docId) {
		int colonIndex = docId.indexOf(":");
		if (colonIndex < 0) return docId;
		return RESOURCE_PREFIX+docId.substring(colonIndex+1);
	}

	public static InfModel getInferredModel(Model m) {
		return ModelFactory.createInfModel(TransferHelpers.bdrcReasoner, m);
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
			FusekiHelpers.transferModel(CORE_PREFIX+"ontologySchema", ontModel);
		} catch (TimeoutException e) {
			logger.error("Timeout sending ontology model", e);
		}
	}
}
