package io.bdrc.gittodbs;

import static io.bdrc.libraries.Models.getMd5;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.io.FileInputStream;
import java.nio.file.Paths;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.ontology.OntDocumentManager;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.util.Context;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.InvalidObjectIdException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransferHelpers {
    public static final String RESOURCE_PREFIX = "http://purl.bdrc.io/resource/";
    public static final String GRAPH_PREFIX = "http://purl.bdrc.io/graph/";
	public static final String CORE_PREFIX = "http://purl.bdrc.io/ontology/core/";
	public static final String CONTEXT_URL = "http://purl.bdrc.io/context.jsonld";
	public static final String ADMIN_PREFIX = "http://purl.bdrc.io/ontology/admin/";
    public static final String DATA_PREFIX = "http://purl.bdrc.io/data/";
    public static final String ADMIN_DATA_PREFIX = "http://purl.bdrc.io/admindata/";
    public static final String SKOS_PREFIX = "http://www.w3.org/2004/02/skos/core#";
    public static final String VCARD_PREFIX = "http://www.w3.org/2006/vcard/ns#";
    public static final String TBR_PREFIX = "http://purl.bdrc.io/ontology/toberemoved/";
    public static final String OWL_PREFIX = "http://www.w3.org/2002/07/owl#";
    public static final String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#";
    public static final String XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";
    
    public static final String BDA = ADMIN_DATA_PREFIX;
    public static final String BDG = GRAPH_PREFIX;
    public static final String BDR = RESOURCE_PREFIX;
    public static final String BDO = CORE_PREFIX;
    public static final String ADM = ADMIN_PREFIX;
    public static final String BDGU = "http://purl.bdrc.io/graph-nc/user/";
    public static final String BDGUP = "http://purl.bdrc.io/graph-nc/user-private/";
    
    public static final Context ctx = new Context();
    
    private static Map<String, DocType> strToDocType = new HashMap<>();
    public enum DocType {

        CORPORATION("corporation"), 
        EINSTANCE("einstance"), 
        ETEXT("etext"), 
        ETEXTCONTENT("etextcontent"),
        IINSTANCE("iinstance"),
        INSTANCE("instance"), 
        ITEM("item"), 
        LINEAGE("lineage"), 
        OFFICE("role"), 
        PERSON("person"),
        PLACE("place"), 
        COLLECTION("collection"), 
        SUBSCRIBER("subscriber"),
        TOPIC("topic"),
        WORK("work"),
        OUTLINE("outline"),
        USER("user"),
        USER_PRIVATE("user-private"),
        TEST("test");

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
    
	public static Logger logger = LoggerFactory.getLogger(TransferHelpers.class);
	
	public static boolean progress = false;
	
	public static long TRANSFER_TO = 50; // seconds
	
	public static ExecutorService executor = Executors.newCachedThreadPool();

	public static String adminNS = "http://purl.bdrc.io/ontology/admin/";
	public static OntModel ontModel = null;
	public static Reasoner bdrcReasoner = null;
	
	public static void init() throws MalformedURLException {

	    if (GitToDB.transferFuseki || GitToDB.transferES) {
	        // we need it also for the cache in ES export
	        FusekiHelpers.init(GitToDB.fusekiHost, GitToDB.fusekiPort, GitToDB.fusekiName, GitToDB.fusekiAuthName);	        
	    }
		
	    ontModel = getOntologyModel();
		bdrcReasoner = BDRCReasoner.getReasoner(ontModel, true);
	}
	
	public static void sync(int howMany) {
		TransferHelpers.logger.info("start sync, ric is {}, limit is {}", GitToDB.ric, howMany);
	    int nbLeft = howMany;
	    nbLeft = nbLeft - syncType(DocType.PERSON, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.ITEM, nbLeft);
        nbLeft = nbLeft - syncType(DocType.WORK, nbLeft);
        nbLeft = nbLeft - syncType(DocType.IINSTANCE, nbLeft);
        nbLeft = nbLeft - syncType(DocType.INSTANCE, nbLeft);
        nbLeft = nbLeft - syncType(DocType.OUTLINE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.CORPORATION, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.PLACE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.TOPIC, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.LINEAGE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.COLLECTION, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.OFFICE, nbLeft);
        nbLeft = nbLeft - syncType(DocType.EINSTANCE, nbLeft);
	    nbLeft = nbLeft - syncType(DocType.ETEXTCONTENT, nbLeft);
	    if (!GitToDB.ric) {
	    	nbLeft = nbLeft - syncType(DocType.USER, nbLeft);
	    	nbLeft = nbLeft - syncType(DocType.USER_PRIVATE, nbLeft);
	    }
	    FusekiHelpers.closeConnection(FusekiHelpers.CORE);
	    if (!GitToDB.ric) {
	        syncType(DocType.SUBSCRIBER, nbLeft);
	        FusekiHelpers.closeConnection(FusekiHelpers.AUTH);
	    }
	}
	
	public static void closeConnections() {
	    if (GitToDB.transferFuseki)
	        FusekiHelpers.closeConnections();
	}
	
	public static List<DocType> esDts = Arrays.asList(new DocType[] {DocType.COLLECTION, DocType.PERSON, DocType.PLACE, DocType.INSTANCE, DocType.OUTLINE, DocType.TOPIC});
	public static int syncType(DocType type, int nbLeft) {
	    int i = 0;
	    // random result for uncoherent couch and fuseki
	    if (GitToDB.transferFuseki)
	        i = syncTypeFuseki(type, nbLeft);
	    if (GitToDB.transferES && esDts.contains(type))
            i = syncTypeES(type, nbLeft);
	    return i;
	}
	
    public static Model getPublicUserModel(final Dataset ds) {
        final Iterator<String> graphUrisIt = ds.listNames();
        Model res = ModelFactory.createDefaultModel();
        while (graphUrisIt.hasNext()) {
            final String graphUri = graphUrisIt.next();
            if (graphUri.startsWith(BDA) || graphUri.startsWith(BDGU)) {
                res.add(ds.getNamedModel(graphUri));
            }
        }
        return res;
    }
    
    public static Model getPrivateUserModel(final Dataset ds) {
        final Iterator<String> graphUrisIt = ds.listNames();
        Model res = ModelFactory.createDefaultModel();
        while (graphUrisIt.hasNext()) {
            final String graphUri = graphUrisIt.next();
            if (graphUri.startsWith(BDA) || graphUri.startsWith(BDGUP)) {
                res.add(ds.getNamedModel(graphUri));
            }
        }
        return res;
    }
	
	public static Model modelFromPath(String path, DocType type, String mainId) {
	    if (type == DocType.ETEXTCONTENT) {
	        String dirpath = GitToDB.gitDir + DocType.ETEXT + "s" + GitHelpers.localSuffix + "/"+getMd5(mainId)+"/";	        
	        Model etextM = modelFromPath(dirpath+mainId+".trig", DocType.ETEXT, mainId);	        
	        Model res = EtextContents.getModel(path, mainId, etextM);
            return res;
	    }
	    Model model = ModelFactory.createDefaultModel();
        try {
            Dataset dataset = RDFDataMgr.loadDataset(path);
            if (type == DocType.USER) {
            	model = getPublicUserModel(dataset);
            } else if (type == DocType.USER_PRIVATE) {
            	model = getPrivateUserModel(dataset);
            } else {
	            Iterator<String> iter = dataset.listNames();
	            if (iter.hasNext()) {
	                String graphUri = iter.next();
	                if (iter.hasNext())
	                    logger.error("modelFromFileName " + path + " getting named model: " + graphUri + ". Has more graphs! ");
	                model = dataset.getNamedModel(graphUri);
	            }
            }
        } catch (RiotException e) {
            logger.error("error reading "+path);
            return null;
        }
        setPrefixes(model, type);
        return model;
	}
	
	   public static Dataset datasetFromPath(String path, DocType type, String mainId) {
	        Dataset dataset = null;
            try {
                dataset = RDFDataMgr.loadDataset(path);
            } catch (RiotException e) {
                logger.error("error reading "+path);
                return null;
            }
	        return dataset;
	    }
	
	public static String mainIdFromPath(String path, DocType type) {
        if (path == null || path.length() < 6)
            return null;
        if (type != DocType.ETEXTCONTENT && !(path.endsWith(".ttl") || path.endsWith(".trig")))
            return null;
        if (type == DocType.ETEXTCONTENT && !path.endsWith(".txt"))
            return null;
        if (path.charAt(2) == '/')
            return path.substring(3, path.length()-(path.endsWith(".trig") ? 5 : 4));
        return path.substring(0, path.length() - (path.endsWith(".trig") ? 5 : 4));
	}
	
	   public static void addSingleFileFuseki(final String filePath, final String defaultGraphName) {
	       logger.info("add single file "+filePath);
	       final Dataset dataset;
	       if (filePath.endsWith(".gz")) {
	           GZIPInputStream gis;
	           try {
	               gis = new GZIPInputStream(new FileInputStream(Paths.get(filePath).toFile()));
               } catch (IOException e1) {
                   logger.error("error reading "+filePath);
                   return;
               }
	           try {
	               dataset = DatasetFactory.createTxnMem();
	               RDFParser.create()
	                   .source(gis)
	                   .lang(Lang.TURTLE)
	                   .parse(dataset);
               } catch (RiotException e) {
                   logger.error("error reading "+filePath);
                   return;
               }
	       } else {
    	       try {
                   dataset = RDFDataMgr.loadDataset(filePath);
               } catch (RiotException e) {
                   logger.error("error reading "+filePath);
                   return;
               }
	       }
	       final Iterator<String> ir = dataset.listNames();
	       while (ir.hasNext()) {
	           final String gn = ir.next();
	           final Model origModel = dataset.getNamedModel(gn);
	           FusekiHelpers.postModelInParts(gn, origModel, FusekiHelpers.CORE);
	       }
	       if (defaultGraphName != null)
	           FusekiHelpers.postModelInParts(defaultGraphName, dataset.getDefaultModel(), FusekiHelpers.CORE);
	    }
	
	public static void addFile(DocType type, String dirPath, String filePath) {
        final String mainId = mainIdFromPath(filePath, type);
        if (mainId == null)
            return;
        if (GitToDB.transferES && ESUtils.shouldIgore(mainId))
            return;
        Model model = modelFromPath(dirPath+filePath, type, mainId);
        if (model == null) { // nothing fetched from path, nothing to transfer
            logger.error("modelFromPath failed to fetch anything from: " + dirPath+filePath + " with type: " + type + " and mainId: " + mainId);
            return;
        }
        if (GitToDB.transferFuseki)
            transferToFuseki(type, mainId, filePath, model, BDG+mainId);
        if (GitToDB.transferES)
            ESUtils.upload(type, mainId, filePath, model, BDG+mainId);
	}
	
	   public static void addUserFileFuseki(DocType type, String dirPath, String filePath) {
	        final String mainId = mainIdFromPath(filePath, type);
	        if (mainId == null)
	            return;
	        final Dataset ds = datasetFromPath(dirPath+filePath, type, mainId);
	        if (ds == null) { // nothing fetched from path, nothing to transfer
	            logger.error("modelFromPath failed to fetch anything from: " + dirPath+filePath + " with type: " + type + " and mainId: " + mainId);
	            return;
	        }
	        final Iterator<String> graphUrisIt = ds.listNames();
	        while (graphUrisIt.hasNext()) {
	            final String graphUri = graphUrisIt.next();
	            if (graphUri.startsWith(BDA) || graphUri.startsWith(BDGUP)) {
	                // Private
	                transferToFuseki(DocType.USER_PRIVATE, mainId, filePath, ds.getNamedModel(graphUri), graphUri);
	            } else {
	                transferToFuseki(type, mainId, filePath, ds.getNamedModel(graphUri), graphUri);
	            }
	        }
	    }
	
	public static void transferToFuseki(final DocType type, final String mainId, final String filePath, Model model, final String graphName) {
	    final String rev = GitHelpers.getLastRefOfFile(type, filePath); // not sure yet what to do with it
        FusekiHelpers.setModelRevision(model, type, rev, mainId);
        // apply reasoner only to released models
        if (type != DocType.ETEXTCONTENT && type != DocType.ETEXT && isReleased(model)) {
            model = getInferredModel(model);
        }
        FusekiHelpers.transferModel(type, graphName, model);
	}

	public static void logFileHandling(int i, final String path, boolean fuseki) {
	    TransferHelpers.logger.debug("sending "+path+" to "+(fuseki ? "Fuseki" : "ES"));
	    if (i % 100 == 0 && progress)
	        logger.info(path + ":" + i);
	}
	
	public static int syncAllHead(final DocType type, int nbLeft, final String dirpath) {
	    final TreeWalk tw = GitHelpers.listRepositoryContents(type);
        TransferHelpers.logger.info("sending all " + type + " files");
        int i = 0;
        try {
            while (tw.next()) {
                if (i+1 > nbLeft)
                    return nbLeft;
                i = i + 1;
                logFileHandling(i, tw.getPathString(), GitToDB.transferFuseki);
                addFile(type, dirpath, tw.getPathString());
            }
            if (GitToDB.transferFuseki)
                FusekiHelpers.finishDatasetTransfers(FusekiHelpers.distantDB(type));
        } catch (IOException e) {
            TransferHelpers.logger.error("syncAllHead", e);
            return 0;
        }
        return i;
	}
	
	public static int syncTypeFuseki(final DocType type, int nbLeft) {
	    if (nbLeft == 0) {
	    	TransferHelpers.logger.info("not syncing {}", type);
	        return 0;
	    }
	    final String gitRev = GitHelpers.getHeadRev(type);
	    String typeStr = type.toString();
	    if (type == DocType.USER_PRIVATE)
	    	typeStr = "user";
        final String dirpath = GitToDB.gitDir + typeStr + "s" + GitHelpers.localSuffix + "/";
	    if (gitRev == null) {
	        TransferHelpers.logger.error("cannot extract latest revision from the git repo at "+dirpath);
	        return 0;
	    }
	    String distRev = GitToDB.sinceCommit;
	    if (distRev == null)
	        distRev = FusekiHelpers.getLastRevision(type);
	    int i = 0;
	    if (distRev == null || distRev.isEmpty() || GitToDB.force) {
	        i = syncAllHead(type, nbLeft, dirpath);
	    } else if (!GitHelpers.hasRev(type, distRev)) {
	        TransferHelpers.logger.error("distant fuseki revision "+distRev+" is not found in the git repo, sending all files.");
            i = syncAllHead(type, nbLeft, dirpath);
	    } else {
	        List<DiffEntry> entries = null;
	        try {
	            entries = GitHelpers.getChanges(type, distRev);
	            TransferHelpers.logger.info("sending "+entries.size()+" changed " + type + " files since "+distRev+" to Fuseki");
	            for (DiffEntry de : entries) {
	                i++;
	                if (i > nbLeft)
	                    return nbLeft;
	                final String newPath = de.getNewPath();
	                logFileHandling(i, newPath, true);
	                final String oldPath = de.getOldPath();
	                    
	                if (newPath.equals("/dev/null") || !newPath.equals(oldPath)) {
	                    final String mainId = mainIdFromPath(oldPath, type);
	                    if (mainId != null) {
	                    	String graphName = BDG+mainId;
	                    	if (type == DocType.USER) {
	                    		graphName = BDGU + mainId;
	                    	} else if (type == DocType.USER_PRIVATE) {
	                    		graphName = BDGUP + mainId;
	                    	}
	                        FusekiHelpers.deleteModel(graphName, FusekiHelpers.distantDB(type));
	                    }
	                }
	                if (!newPath.equals("/dev/null"))
	                    addFile(type, dirpath, newPath);
	            }
            } catch (InvalidObjectIdException | RevisionSyntaxException | IOException e) {
                TransferHelpers.logger.error("Git unknown error, this shouldn't happen.", e);
                i = 0;
            }
	    }
	    FusekiHelpers.setLastRevision(gitRev, type);
	    return i;
	}
	
	public static int syncTypeES(final DocType type, int nbLeft) {
        if (nbLeft == 0) {
            TransferHelpers.logger.info("not syncing {}", type);
            return 0;
        }
        final String gitRev = GitHelpers.getHeadRev(type);
        String typeStr = type.toString();
        if (type == DocType.USER_PRIVATE)
            return 0;
        final String dirpath = GitToDB.gitDir + typeStr + "s" + GitHelpers.localSuffix + "/";
        if (gitRev == null) {
            TransferHelpers.logger.error("cannot extract latest revision from the git repo at "+dirpath);
            return 0;
        }
        String distRev = GitToDB.sinceCommit;
        if (distRev == null)
            distRev = ESUtils.getLastRevision(type);
        int i = 0;
        if (distRev == null || distRev.isEmpty() || GitToDB.force) {
            i = syncAllHead(type, nbLeft, dirpath);
        } else if (!GitHelpers.hasRev(type, distRev)) {
            TransferHelpers.logger.error("distant fuseki revision "+distRev+" is not found in the git repo, sending all files.");
            i = syncAllHead(type, nbLeft, dirpath);
        } else {
            List<DiffEntry> entries = null;
            try {
                entries = GitHelpers.getChanges(type, distRev);
                TransferHelpers.logger.info("sending "+entries.size()+" changed " + type + " files since "+distRev+" to Fuseki");
                for (DiffEntry de : entries) {
                    i++;
                    if (i > nbLeft)
                        return nbLeft;
                    final String newPath = de.getNewPath();
                    logFileHandling(i, newPath, true);
                    final String oldPath = de.getOldPath();
                        
                    if (newPath.equals("/dev/null") || !newPath.equals(oldPath)) {
                        final String mainId = mainIdFromPath(oldPath, type);
                        if (mainId != null)
                            ESUtils.remove(mainId, type);
                    }
                    if (!newPath.equals("/dev/null"))
                        addFile(type, dirpath, newPath);
                }
            } catch (InvalidObjectIdException | RevisionSyntaxException | IOException e) {
                TransferHelpers.logger.error("Git unknown error, this shouldn't happen.", e);
                i = 0;
            }
        }
        ESUtils.finishDatasetTransfers();
        ESUtils.setLastRevision(gitRev, type);
        return i;
    }

	
	public static String getFullUrlFromDocId(String docId) {
		int colonIndex = docId.indexOf(":");
		if (colonIndex < 0) return docId;
		return RESOURCE_PREFIX+docId.substring(colonIndex+1);
	}

	public static Model getInferredModel(final Model m) {
		BDRCReasoner.addFromEDTF(m);
	    if (bdrcReasoner == null)
	        return m;
		return ModelFactory.createInfModel(bdrcReasoner, m);
	}

	static final Property ricP = ResourceFactory.createProperty( ADM, "restrictedInChina" );
	static final Property access = ResourceFactory.createProperty( ADM , "access" );
	static final Resource accessOpen = ResourceFactory.createProperty( BDA , "AccessOpen" );
	static final Property copyrightStatus = ResourceFactory.createProperty( BDO , "copyrightStatus" );
	static final Resource inCopyright = ResourceFactory.createProperty( BDR , "CopyrightInCopyright" );
	static final Resource copyrightClaimed = ResourceFactory.createProperty( BDR , "CopyrightClaimed" );
	static final public Property status = ResourceFactory.createProperty(ADM+"status");
	static public final Resource statusReleased = ResourceFactory.createProperty(BDA+"StatusReleased");
	static public final Resource statusWithdrawn = ResourceFactory.createProperty(BDA+"StatusWithdrawn");
	static final public Property digitalLendingPossible = ResourceFactory.createProperty(BDO+"digitalLendingPossible");
	
    public static boolean isRic(Model m) {
        if (!m.contains(null, status, statusReleased))
            return true;
        if (m.contains(null, access, (RDFNode) null) && !m.contains(null, access, accessOpen))
            return true;
        if (m.listResourcesWithProperty(digitalLendingPossible, false).hasNext())
            return true;
        return m.listResourcesWithProperty(ricP, true).hasNext();
    }
    
    public static boolean isReleased(Model m) {
        return m.contains(null, status, statusReleased);
    }
    
    public static boolean isWithdrawn(Model m) {
        return m.contains(null, status, statusWithdrawn);
    }

    public static Map<String,Boolean> lnameRic = new HashMap<>();
	public static boolean DRYRUN = false; 
    
    public static void tagAsRic(final String einstLname) {
        lnameRic.put(einstLname, true);
    }
    
    static final Property eiEI = ResourceFactory.createProperty( ADMIN_PREFIX, "eTextInInstance" );
    public static boolean isInRicEInstance(Model m) {
        NodeIterator ri = m.listObjectsOfProperty(eiEI);
        if (!ri.hasNext())
            return false;
        Resource einstance = ri.next().asResource();
        return lnameRic.containsKey(einstance.getLocalName());
    }
    
	// for debugging purposes only
	public static void printModel(Model m) {
		RDFDataMgr.write(System.out, m, RDFFormat.TURTLE_PRETTY);
	}

	public static OntModel getOntologyModel() {
//        OntDocumentManager ontManager = new OntDocumentManager("owl-schema/ont-policy.rdf;https://raw.githubusercontent.com/buda-base/owl-schema/master/ont-policy.rdf");
        OntDocumentManager ontManager = new OntDocumentManager(GitToDB.ontRoot+"ont-policy.rdf");
        ontManager.setProcessImports(true); // not really needed since ont-policy sets it, but what if someone changes the policy
	    
	    OntModelSpec ontSpec = new OntModelSpec( OntModelSpec.OWL_DL_MEM );
	    ontSpec.setDocumentManager( ontManager );	        
	    
	    OntModel ontModel = ontManager.getOntology( adminNS, ontSpec );

	    logger.info("getOntologyModel ontModel.size() = " + ontModel.size());
	    return ontModel;
	}

	public static void transferOntology() {
	    logger.info("Transferring Ontology: " + CORE_PREFIX+"ontologySchema");
	    FusekiHelpers.transferModel(null, BDG+"ontologySchema", ontModel, true);
	}

    public static void check_consistency(final DocType docType, final String sinceCommit) {
        final Map<String,String> ridToRevGit = new HashMap<>();
        final Map<String,String> ridToGitPath = new HashMap<>();
        final Map<String,String> ridToRevFuseki = new HashMap<>();
        final Repository r = GitHelpers.typeRepo.get(docType);
        final int db = FusekiHelpers.distantDB(docType);
        if (r == null) {
            logger.error("can't find repository");
            return;
        }
        String typeStr = docType.toString();
        if (docType == DocType.USER_PRIVATE)
            typeStr = "user";
        final String dirpath = GitToDB.gitDir + typeStr + "s" + GitHelpers.localSuffix + "/";
        try {
            final Git git = new Git(r);
            final LogCommand lc = git.log();
            if (sinceCommit != null) {
                final ObjectId headCommitId = r.resolve("HEAD^{commit}");
                final ObjectId sinceCommitId = r.resolve(sinceCommit+"^{commit}");
                lc.addRange(sinceCommitId, headCommitId);
            }
            logger.info("get logs between {}^{commit} to HEAD^{commit}", sinceCommit);
            final Iterable<RevCommit> logs = lc.call();
            for (final RevCommit rc : logs) {
                if (rc.getParentCount() == 0)
                    break;
                if (rc.getParentCount() > 1) {
                    logger.error("rev {} has more than one parent, skipping merges");
                    break;
                }
                final List<DiffEntry> entries = GitHelpers.getChanges(docType, rc.getParent(0).getName(), rc.getName());
                for (final DiffEntry de : entries) {
                    final String newPath = de.getNewPath();
                    final String oldPath = de.getOldPath();
                    if (newPath.equals("/dev/null") || !newPath.equals(oldPath)) {
                        final String mainId = mainIdFromPath(oldPath, docType);
                        if (mainId == null || ridToRevGit.containsKey(mainId))
                            continue;
                        ridToRevGit.put(mainId, null);
                        ridToGitPath.put(mainId, newPath);
                        logger.debug(oldPath+" removed from git");
                    }
                    if (!newPath.equals("/dev/null")) {
                        final String mainId = mainIdFromPath(newPath, docType);
                        if (mainId == null || ridToRevGit.containsKey(mainId))
                            continue;
                        ridToGitPath.put(mainId, newPath);
                        ridToRevGit.put(mainId, rc.getName());
                        logger.info(newPath+" has git revision "+rc.name());
                    }
                }
            }
            git.close();
        } catch (RevisionSyntaxException | IOException | GitAPIException e) {
            logger.error("can't run git command");
            return;
        }
        logger.info("got git revisions for "+ridToRevGit.size()+" files");
        // TODO: handle users
        FusekiHelpers.getRevisionsBatch(new ArrayList<String>(ridToRevGit.keySet()), ridToRevFuseki, 20, db);
        logger.info("got {} revisions from Fuseki", ridToRevFuseki.size());
        for (final Map.Entry<String,String> e : ridToRevGit.entrySet()) {
            final String rid = e.getKey();
            final String gitRev = e.getValue();
            String fusekiRev = null;
            final String graphName = "http://purl.bdrc.io/graph/"+rid;
            if (ridToRevFuseki.containsKey(rid))
                fusekiRev = ridToRevFuseki.get(rid);
            if (fusekiRev.equals(gitRev))
                continue;
            if (gitRev != null) {
                logger.info("inconsistency on {}: {} -> {}", rid, fusekiRev, gitRev);
                addFile(docType, dirpath, ridToGitPath.get(rid));
            }
            else
                FusekiHelpers.deleteModel(graphName, db);
        }
    }
}
