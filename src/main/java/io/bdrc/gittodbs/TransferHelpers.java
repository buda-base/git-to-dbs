package io.bdrc.gittodbs;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.jena.graph.Graph;
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
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.treewalk.TreeWalk;
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
	
	public static enum DocType {
	    CORPORATION,
	    LINEAGE,
	    OFFICE,
	    PERSON,
	    PLACE,
	    TOPIC,
	    ITEM,
	    WORK,
	    PRODUCT,
	    TEST
	    ;  
	  }
	
	public static final Map<DocType, String> typeToStr = new EnumMap<>(DocType.class);
	
	static {
	    typeToStr.put(DocType.CORPORATION, "corporation");
	    typeToStr.put(DocType.LINEAGE, "lineage");
	    typeToStr.put(DocType.OFFICE, "office");
	    typeToStr.put(DocType.PERSON, "person");
	    typeToStr.put(DocType.PLACE, "place");
	    typeToStr.put(DocType.TOPIC, "topic");
	    typeToStr.put(DocType.ITEM, "item");
	    typeToStr.put(DocType.WORK, "work");
	    typeToStr.put(DocType.PRODUCT, "product");
	    typeToStr.put(DocType.TEST, "test");
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
	    syncType(DocType.CORPORATION);
//	    syncType(DocType.PERSON);
//	    syncType(DocType.WORK);
//	    syncType(DocType.PLACE);
//	    syncType(DocType.TOPIC);
//	    syncType(DocType.LINEAGE);
//	    syncType(DocType.PRODUCT);
//	    syncType(DocType.ITEM);
//	    syncType(DocType.OFFICE);
	}
	
	public static void syncType(DocType type) {
	    
	}
	
    public static void setPrefixes(Model m, DocType type) {
        m.setNsPrefix("", CORE_PREFIX);
        m.setNsPrefix("adm", ADMIN_PREFIX);
        m.setNsPrefix("bdd", DATA_PREFIX);
        m.setNsPrefix("bdr", RESOURCE_PREFIX);
        m.setNsPrefix("tbr", TBR_PREFIX);
        m.setNsPrefix("owl", OWL_PREFIX);
        m.setNsPrefix("rdf", RDF_PREFIX);
        m.setNsPrefix("rdfs", RDFS_PREFIX);
        m.setNsPrefix("skos", SKOS_PREFIX);
        m.setNsPrefix("xsd", XSD_PREFIX);
        if (type == DocType.PLACE)
            m.setNsPrefix("vcard", VCARD_PREFIX);
    }
	
	public static Model modelFromPath(String path, DocType type) {
        Model model = ModelFactory.createDefaultModel();
        Graph g = model.getGraph();
        try {
            // workaround for https://github.com/jsonld-java/jsonld-java/issues/199
            RDFParserBuilder pb = RDFParser.create()
                     .source(path)
                     .lang(RDFLanguages.TTL);
                     //.canonicalLiterals(true);
            pb.parse(StreamRDFLib.graph(g));
        } catch (RiotException e) {
            TransferHelpers.logger.error("error reading "+path);
            return null;
        }
        setPrefixes(model, type);
        return model;
	}
	
	public static String mainIdFromPath(String path) {
        if (path == null || path.length() < 6 || !path.endsWith(".ttl"))
            return null;
        if (path.charAt(2) == '/')
            return path.substring(3, path.length()-4);
        return path.substring(0, path.length()-4);
	}
	
	public static void addFileFuseki(DocType type, String dirPath, String filePath) {
        String mainId = mainIdFromPath(filePath);
        if (mainId == null)
            return;
        Model m = modelFromPath(dirPath+filePath, type);
        //String rev = GitHelpers.getLastRefOfFile(type, filePath); // not sure yet what to do with it
        // TODO: get inferred model
        try {
            FusekiHelpers.transferModel(BDR+mainId, m);
        } catch (TimeoutException e) {
            TransferHelpers.logger.error("", e);
        }
	}
	
	public static void syncTypeFuseki(DocType type) {
	    String gitRev = GitHelpers.getHeadRev(type);
        String dirpath = GitToDB.gitDir+TransferHelpers.typeToStr.get(type)+"s/";
	    if (gitRev == null) {
	        TransferHelpers.logger.error("cannot extract latest revision from the git repo at "+dirpath);
	        return;
	    }
	    String distRev = FusekiHelpers.getLastRevision(type);
	    if (distRev == null || distRev.isEmpty()) {
	        TreeWalk tw = GitHelpers.listRepositoryContents(type);
	        try {
                while (tw.next()) {
                    System.out.println(tw.getPathString());
                    addFileFuseki(type, dirpath, tw.getPathString());
                }
            } catch (IOException e) {
                TransferHelpers.logger.error("", e);
                return;
            }
	    } else {
	        List<DiffEntry> entries = GitHelpers.getChanges(type, distRev);
	        for (DiffEntry de : entries) {
	            String path = de.getNewPath();
	            System.out.println(path);
	            String oldPath = de.getOldPath();
	            if (path.equals("/dev/null") || !path.equals(oldPath)) {
	                String mainId = mainIdFromPath(oldPath);
	                if (mainId != null) {
    	                try {
                            FusekiHelpers.deleteModel(BDR+mainId);
                        } catch (TimeoutException e) {
                            TransferHelpers.logger.error("", e);
                        }
	                }
	            }
	            if (!path.equals("/dev/null"))
	                addFileFuseki(type, dirpath, path);
	        }
	    }
	    FusekiHelpers.setLastRevision(gitRev, type);
	}
	
	public static void addFileCouch(DocType type, String dirPath, String filePath) {
        String mainId = mainIdFromPath(filePath);
        if (mainId == null)
            return;
        Model m = modelFromPath(dirPath+filePath, type);
        String rev = GitHelpers.getLastRefOfFile(type, filePath); // not sure yet what to do with it
        Map<String,Object> jsonObject = JSONLDFormatter.modelToJsonObject(m, type, mainId);
        CouchHelpers.jsonObjectToCouch(jsonObject, mainId, type, rev);
    }
	
	public static void syncTypeCouch(DocType type) {
        String gitRev = GitHelpers.getHeadRev(type);
        String dirpath = GitToDB.gitDir+TransferHelpers.typeToStr.get(type)+"s/";
        if (gitRev == null) {
            TransferHelpers.logger.error("cannot extract latest revision from the git repo at "+dirpath);
            return;
        }
        String distRev = CouchHelpers.getLastRevision(type);
        if (distRev == null || distRev.isEmpty()) {
            TreeWalk tw = GitHelpers.listRepositoryContents(type);
            try {
                while (tw.next()) {
                    addFileCouch(type, dirpath, tw.getPathString());
                }
            } catch (IOException e) {
                TransferHelpers.logger.error("", e);
                return;
            }
        } else {
            List<DiffEntry> entries = GitHelpers.getChanges(type, distRev);
            for (DiffEntry de : entries) {
                String path = de.getNewPath();
                String oldPath = de.getOldPath();
                if (path.equals("/dev/null") || !path.equals(oldPath)) {
                    String mainId = mainIdFromPath(oldPath);
                    if (mainId != null) {
                        try {
                            FusekiHelpers.deleteModel(BDR+mainId);
                        } catch (TimeoutException e) {
                            TransferHelpers.logger.error("", e);
                        }
                    }
                }
                if (!path.equals("/dev/null"))
                    addFileFuseki(type, dirpath, path);
            }
        }
        CouchHelpers.setLastRevision(gitRev, type);
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
