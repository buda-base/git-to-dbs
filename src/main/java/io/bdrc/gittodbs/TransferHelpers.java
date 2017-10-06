package io.bdrc.gittodbs;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.InvalidObjectIdException;
import org.eclipse.jgit.errors.MissingObjectException;
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
    public static final String ADM = ADMIN_PREFIX;
    public static final Context ctx = new Context();
    static MessageDigest md;
    private static final int hashNbChars = 2;
	
	public static enum DocType {
	    CORPORATION,
	    LINEAGE,
	    ETEXT,
	    ETEXTCONTENT,
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
	    typeToStr.put(DocType.ETEXT, "etext");
	    typeToStr.put(DocType.ETEXTCONTENT, "etextcontent");
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
	
    public static void setPrefixes(Model m, DocType type) {
        m.setNsPrefix("", CORE_PREFIX);
        m.setNsPrefix("adm", ADMIN_PREFIX);
       // m.setNsPrefix("bdd", DATA_PREFIX);
        m.setNsPrefix("bdr", RESOURCE_PREFIX);
        m.setNsPrefix("tbr", TBR_PREFIX);
        //m.setNsPrefix("owl", OWL_PREFIX);
        m.setNsPrefix("rdf", RDF_PREFIX);
        m.setNsPrefix("rdfs", RDFS_PREFIX);
        m.setNsPrefix("skos", SKOS_PREFIX);
        m.setNsPrefix("xsd", XSD_PREFIX);
        if (type == DocType.PLACE)
            m.setNsPrefix("vcard", VCARD_PREFIX);
    }
    
	public static Logger logger = LoggerFactory.getLogger("git2dbs");
	
	public static boolean progress = false;
	
	public static long TRANSFER_TO = 50; // seconds
	
	public static ExecutorService executor = Executors.newCachedThreadPool();


	public static OntModel ontModel = null;
	public static Model baseModel = null;
	public static Reasoner bdrcReasoner = null;
	
	public static void init() throws MalformedURLException {
	    try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
	    if (GitToDB.transferFuseki) {
	        FusekiHelpers.init(GitToDB.fusekiHost, GitToDB.fusekiPort, GitToDB.fusekiName);	        
	    }
	    if (GitToDB.transferCouch) {
	        CouchHelpers.init(GitToDB.couchdbHost, GitToDB.couchdbPort, GitToDB.libFormat);
	    }
		baseModel = getOntologyBaseModel(); 
		ontModel = getOntologyModel(baseModel);
		bdrcReasoner = BDRCReasoner.getReasoner(ontModel);
	}
	
	public static void sync(int howMany) {
	    int nbLeft = howMany;
	    nbLeft = nbLeft - syncType(DocType.PERSON, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.ITEM, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.WORK, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.CORPORATION, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.PLACE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.TOPIC, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.LINEAGE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.PRODUCT, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.OFFICE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.ETEXT, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.ETEXTCONTENT, nbLeft);
	    closeConnections();
	}
	
	public static String getMd5(String resId) {
	    try {
            // keeping files from the same work together:
            final int underscoreIndex = resId.indexOf('_');
            String message = resId;
            if (underscoreIndex != -1)
                message = resId.substring(0, underscoreIndex);
            final byte[] bytesOfMessage = message.getBytes("UTF-8");
            final byte[] hashBytes = md.digest(bytesOfMessage);
            BigInteger bigInt = new BigInteger(1,hashBytes);
            return String.format("%032x", bigInt).substring(0, hashNbChars);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
	}
	
	public static void closeConnections() {
	    if (GitToDB.transferFuseki)
	        FusekiHelpers.closeConnections();
	}
	
	public static int syncType(DocType type, int nbLeft) {
	    int i = 0;
	    // random result for uncoherent couch and fuseki
	    if (GitToDB.transferFuseki)
	        i = syncTypeFuseki(type, nbLeft);
	    if (GitToDB.transferCouch && type != DocType.ETEXTCONTENT)
	        i = syncTypeCouch(type, nbLeft);
	    return i;
	}
	
	public static Model modelFromPath(String path, DocType type, String mainId) {
	    if (type == DocType.ETEXTCONTENT) {
	        Model res = EtextContents.getModel(path, mainId);
	        setPrefixes(res, type);
	        return res;
	    }
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
	
	public static String mainIdFromPath(String path, DocType type) {
        if (path == null || path.length() < 6)
            return null;
        if (type != DocType.ETEXTCONTENT && !path.endsWith(".ttl"))
            return null;
        if (type == DocType.ETEXTCONTENT && !path.endsWith(".txt"))
            return null;
        if (path.charAt(2) == '/')
            return path.substring(3, path.length()-4);
        return path.substring(0, path.length()-4);
	}
	
	public static void addFileFuseki(DocType type, String dirPath, String filePath, boolean firstTransfer) {
        final String mainId = mainIdFromPath(filePath, type);
        if (mainId == null)
            return;
        Model m = modelFromPath(dirPath+filePath, type, mainId);
        final String rev = GitHelpers.getLastRefOfFile(type, filePath); // not sure yet what to do with it
        FusekiHelpers.setModelRevision(m, type, rev, mainId);
        m = getInferredModel(m);
        String graphName = BDR+mainId;
        if (type == DocType.ETEXTCONTENT)
            graphName += "_STR";
        try {
            FusekiHelpers.transferModel(graphName, m, firstTransfer);
        } catch (TimeoutException e) {
            TransferHelpers.logger.error("", e);
        }
	}
	
	public static void logFileHandling(int i, String path, boolean fuseki) {
	    TransferHelpers.logger.debug("sending "+path+" to "+(fuseki ? "Fuseki" : "Couchdb"));
	    if (i % 100 == 0 && progress)
	        logger.info(path + ":" + i);
	}
	
	public static int syncTypeFuseki(DocType type, int nbLeft) {
	    if (nbLeft == 0)
	        return 0;
	    String gitRev = GitHelpers.getHeadRev(type);
        String dirpath = GitToDB.gitDir+TransferHelpers.typeToStr.get(type)+"s/";
	    if (gitRev == null) {
	        TransferHelpers.logger.error("cannot extract latest revision from the git repo at "+dirpath);
	        return 0;
	    }
	    String distRev = FusekiHelpers.getLastRevision(type);
	    int i = 0;
	    if (distRev == null || distRev.isEmpty()) {
	        TreeWalk tw = GitHelpers.listRepositoryContents(type);
	        TransferHelpers.logger.info("sending all "+typeToStr.get(type)+" files to Fuseki");
	        try {
                while (tw.next()) {
                    if (i+1 > nbLeft)
                        return nbLeft;
                    i = i + 1;
                    logFileHandling(i, tw.getPathString(), true);
                    addFileFuseki(type, dirpath, tw.getPathString(), true);
                }
                FusekiHelpers.finishDatasetTransfers();
            } catch (IOException e) {
                TransferHelpers.logger.error("", e);
                return 0;
            }
	    } else {
	        final List<DiffEntry> entries;
	        try {
	            entries = GitHelpers.getChanges(type, distRev);
	        } catch (InvalidObjectIdException | MissingObjectException e) {
	            TransferHelpers.logger.error("distant fuseki revision "+distRev+" is invalid, please fix it");
	            return 0;
	        }
	        TransferHelpers.logger.info("sending changed "+typeToStr.get(type)+" files changed since "+distRev+" to Fuseki");
	        for (DiffEntry de : entries) {
	            if (i+1 > nbLeft)
	                return nbLeft;
	            i = i + 1;
	            final String path = de.getNewPath();
	            logFileHandling(i, path, true);
	            final String oldPath = de.getOldPath();
	            if (path.equals("/dev/null") || !path.equals(oldPath)) {
	                final String mainId = mainIdFromPath(oldPath, type);
	                if (mainId != null) {
    	                try {
                            FusekiHelpers.deleteModel(BDR+mainId);
                        } catch (TimeoutException e) {
                            TransferHelpers.logger.error("", e);
                        }
	                }
	            }
	            if (!path.equals("/dev/null"))
	                addFileFuseki(type, dirpath, path, false);
	        }
	    }
	    FusekiHelpers.setLastRevision(gitRev, type);
	    return i;
	}
	
	public static void addFileCouch(final DocType type, final String dirPath, final String filePath) {
        final String mainId = mainIdFromPath(filePath, type);
        if (mainId == null)
            return;
        final Model m = modelFromPath(dirPath+filePath, type, mainId);
        final String rev = GitHelpers.getLastRefOfFile(type, filePath); // not sure yet what to do with it
        final Map<String,Object> jsonObject;
        if (GitToDB.libFormat)
            jsonObject = LibFormat.modelToJsonObject(m, type);
        else
            jsonObject = JSONLDFormatter.modelToJsonObject(m, type, mainId);
        if (jsonObject == null)
            return;
        CouchHelpers.jsonObjectToCouch(jsonObject, mainId, type, rev);
    }
	
	public static int syncTypeCouch(DocType type, int nbLeft) {
       if (nbLeft == 0)
            return 0;
        final String gitRev = GitHelpers.getHeadRev(type);
        final String dirpath = GitToDB.gitDir+TransferHelpers.typeToStr.get(type)+"s/";
        if (gitRev == null) {
            TransferHelpers.logger.error("cannot extract latest revision from the git repo at "+dirpath);
            return 0;
        }
        final String distRev = CouchHelpers.getLastRevision(type);
        int i = 0;
        if (distRev == null || distRev.isEmpty()) {
            TransferHelpers.logger.info("sending all "+typeToStr.get(type)+" files to Couch");
            TreeWalk tw = GitHelpers.listRepositoryContents(type);
            try {
                while (tw.next()) {
                    if (i+1 > nbLeft)
                        return nbLeft;
                    i = i + 1;
                    logFileHandling(i, tw.getPathString(), true);
                    addFileCouch(type, dirpath, tw.getPathString());
                }
            } catch (IOException e) {
                TransferHelpers.logger.error("", e);
                return 0;
            }
        } else {
            final List<DiffEntry> entries;
            try {
                entries = GitHelpers.getChanges(type, distRev);
            } catch (InvalidObjectIdException | MissingObjectException e1) {
                TransferHelpers.logger.error("distant couch revision "+distRev+" is invalid, please fix it");
                return 0;
            }
            TransferHelpers.logger.info("sending "+entries.size()+" "+typeToStr.get(type)+" files changed since "+distRev+" to Couch");
            for (DiffEntry de : entries) {
                if (i+1 > nbLeft)
                    return nbLeft;
                i = i + 1;
                final String path = de.getNewPath();
                logFileHandling(i, path, false);
                final String oldPath = de.getOldPath();
                if (path.equals("/dev/null") || !path.equals(oldPath)) {
                    final String mainId = mainIdFromPath(oldPath, type);
                    if (mainId != null) {
                        CouchHelpers.couchDelete(mainId, type);
                    }
                }
                if (!path.equals("/dev/null"))
                    addFileCouch(type, dirpath, path);
            }
        }
        CouchHelpers.setLastRevision(gitRev, type);
        return i;
    }
	
	public static String getFullUrlFromDocId(String docId) {
		int colonIndex = docId.indexOf(":");
		if (colonIndex < 0) return docId;
		return RESOURCE_PREFIX+docId.substring(colonIndex+1);
	}

	public static InfModel getInferredModel(Model m) {
		return ModelFactory.createInfModel(bdrcReasoner, m);
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
			FusekiHelpers.transferModel(CORE_PREFIX+"ontologySchema", ontModel, false);
		} catch (TimeoutException e) {
			logger.error("Timeout sending ontology model", e);
		}
	}
}
