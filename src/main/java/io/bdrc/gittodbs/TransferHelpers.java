package io.bdrc.gittodbs;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.util.Context;
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
    
    private static Map<String, DocType> strToDocType = new HashMap<>();
    public enum DocType {

        ITEM("item"), 
        PERSON("person");

        private String label;

        private DocType(String label) {
            this.label = label;
            strToDocType.put(label, this);
        }

        public static DocType getType(String label) {
            return strToDocType.get(label);
        }
        
        @Override
        public String toString() {
            return label;
        }
    }
	
    public static void setPrefixes(Model m, DocType type) {
        m.setNsPrefix("", CORE_PREFIX);
        m.setNsPrefix("adm", ADMIN_PREFIX);
        m.setNsPrefix("bdr", RESOURCE_PREFIX);
        m.setNsPrefix("rdf", RDF_PREFIX);
        m.setNsPrefix("rdfs", RDFS_PREFIX);
        m.setNsPrefix("skos", SKOS_PREFIX);
        m.setNsPrefix("xsd", XSD_PREFIX);
    }
    
	public static Logger logger = LoggerFactory.getLogger("git2dbs");
	
	public static boolean progress = false;
	
	public static long TRANSFER_TO = 50; // seconds

	public static void init() throws MalformedURLException {
	    FusekiHelpers.init(GitToDB.fusekiHost, GitToDB.fusekiPort, GitToDB.fusekiName);	        
	}
	
	public static void sync(int howMany) {
	    int nbLeft = howMany;
	    nbLeft = nbLeft - syncType(DocType.PERSON, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.ITEM, nbLeft);
	    closeConnections();
	}

	public static void closeConnections() {
	    FusekiHelpers.closeConnections();
	}

	public static int syncType(DocType type, int nbLeft) {
	    return syncTypeFuseki(type, nbLeft);
	}
	
	public static Model modelFromPath(String path, DocType type, String mainId) {
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
        if (path.charAt(2) == '/')
            return path.substring(3, path.length()-4);
        return path.substring(0, path.length()-4);
	}
	
	public static void addFileFuseki(DocType type, String dirPath, String filePath) {
        final String mainId = mainIdFromPath(filePath, type);
        if (mainId == null)
            return;
        Model m = modelFromPath(dirPath+filePath, type, mainId);
        String graphName = BDR+mainId;
        FusekiHelpers.transferModel(graphName, m);
	}

	public static void logFileHandling(int i, String path, boolean fuseki) {
	    TransferHelpers.logger.debug("sending "+path+" to "+(fuseki ? "Fuseki" : "Couchdb"));
	    if (i % 100 == 0 && progress)
	        logger.info(path + ":" + i);
	}
	
	public static int syncTypeFuseki(DocType type, int nbLeft) {
	    if (nbLeft == 0)
	        return 0;
	    String dirpath = GitToDB.gitDir + type + "s/";
	    int i = 0;
	    TreeWalk tw = GitHelpers.listRepositoryContents(type);
	    TransferHelpers.logger.info("sending all " + type + " files to Fuseki");
	    try {
	        while (tw.next()) {
	            if (i+1 > nbLeft)
	                return nbLeft;
	            i = i + 1;
	            logFileHandling(i, tw.getPathString(), true);
	            addFileFuseki(type, dirpath, tw.getPathString());
	        }
	        FusekiHelpers.finishDatasetTransfers();
	    } catch (IOException e) {
	        TransferHelpers.logger.error("", e);
	        return 0;
	    }
	    return i;
	}
}
