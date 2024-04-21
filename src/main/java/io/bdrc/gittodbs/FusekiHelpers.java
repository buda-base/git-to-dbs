package io.bdrc.gittodbs;

import java.net.MalformedURLException;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFuseki;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bdrc.gittodbs.TransferHelpers.DocType;

import static io.bdrc.gittodbs.GitToDB.connectPerTransfer;
import static io.bdrc.libraries.Models.ADM;
import static io.bdrc.libraries.Models.BDA;
import static io.bdrc.libraries.Models.BDG;

public class FusekiHelpers {
    
    public static String FusekiUrl = "http://localhost:13180/fuseki/corerw/data";
    public static String FusekiAuthUrl = "http://localhost:13180/fuseki/authrw/data";
    public static String FusekiSparqlEndpoint = null;
    public static String FusekiAuthSparqlEndpoint = null;
    public static RDFConnection fuConn = null;
    public static RDFConnection fuAuthConn = null;
    public static Dataset testDataset = null;
    private static RDFConnectionRemoteBuilder fuConnBuilder = null;
    private static RDFConnectionRemoteBuilder fuAuthConnBuilder = null;
    public static String baseUrl = null;
    public static String baseAuthUrl = null;
    public static int initialLoadBulkSize = 50000; // the number of triples above which a dataset load is triggered
    public static int esBulkSize = 500;
    public static boolean addGitRevision = true;
    
    public static boolean updatingFuseki = true;
    
    public static String SYSTEM_GRAPH = BDG+"SystemGitSync";
    
    static Dataset currentDataset = null;
    static Dataset currentAuthDataset = null;
    static int triplesInDataset = 0;
    static int triplesInAuthDataset = 0;
    
    public static final int CORE = 0;
    public static final int AUTH = 1;
    
    public static int distantDB(DocType type) {
        if (type == DocType.SUBSCRIBER || type == DocType.USER_PRIVATE)
            return AUTH;
        return CORE;
    }
    
    public static Dataset getCurrentDataset(final int distantDB) {
        if (distantDB == CORE) {
            if (currentDataset == null)
                currentDataset = DatasetFactory.createGeneral();
            return currentDataset;
        }
        if (currentAuthDataset == null)
            currentAuthDataset = DatasetFactory.createGeneral();
        return currentAuthDataset;
    }
    
    public static int getTriplesInDataset(final int distantDB) {
        if (distantDB == CORE)
            return triplesInDataset;
        return triplesInAuthDataset;
    }
    
    public static Logger logger = LoggerFactory.getLogger(FusekiHelpers.class);
    
    private static void initConnectionBuilder() {
        fuConnBuilder = RDFConnectionFuseki.create()
                .destination(baseUrl)
                .queryEndpoint(baseUrl+"/query")
                .gspEndpoint(baseUrl+"/data")
                .updateEndpoint(baseUrl+"/update");
        fuAuthConnBuilder = RDFConnectionFuseki.create()
                .destination(baseAuthUrl)
                .queryEndpoint(baseAuthUrl+"/query")
                .gspEndpoint(baseAuthUrl+"/data")
                .updateEndpoint(baseAuthUrl+"/update");
    }

    public static RDFConnection openConnection(final int distantDB) {
        if (distantDB == CORE) {
            if (fuConn != null) {
                logger.debug("openConnection already connected to fuseki via RDFConnection at "+FusekiUrl);
                return fuConn;
            }
    
            logger.info("openConnection to fuseki via RDFConnection at "+FusekiUrl);
            if (testDataset != null) {
                fuConn = RDFConnection.connect(testDataset);
            } else {
                fuConn = fuConnBuilder.build();
            }
            return fuConn;
        } else {
            if (fuAuthConn != null) {
                logger.debug("openConnection already connected to fuseki via RDFConnection at "+FusekiAuthUrl);
                return fuAuthConn;
            }
    
            logger.info("openConnection to fuseki via RDFConnection at "+FusekiAuthUrl);
            if (testDataset != null) {
                fuAuthConn = RDFConnection.connect(testDataset);
            } else {
                fuAuthConn = fuAuthConnBuilder.build();
            }
            return fuAuthConn;
        }
    }
    
    public static void closeConnections() {
        closeConnection(CORE);
        closeConnection(AUTH);
    }

    public static void closeConnection(final int distantDB) {
        RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        if (conn == null) {
            logger.info("closeConnection already closed for "+FusekiUrl);
            return;
        }

        logger.info("closeConnections fuConn.commit, end, close");
        conn.commit();
        conn.end();
        conn.close();
        
        if (distantDB == CORE)
            fuConn = null;
        else 
            fuAuthConn = null;
    }

    
    public static void init(String fusekiHost, String fusekiPort, String fusekiEndpoint, String fusekiAuthEndpoint) throws MalformedURLException {
        baseUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint;
        baseAuthUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiAuthEndpoint;
        FusekiUrl = baseUrl+"/data";
        FusekiAuthUrl = baseAuthUrl+"/data";
        FusekiSparqlEndpoint = baseUrl+"/query";
        FusekiAuthSparqlEndpoint = baseAuthUrl+"/query";
        initConnectionBuilder();
        openConnection(CORE);
        openConnection(AUTH);
    }
    
    public static synchronized String getLastRevision(DocType type) {
    	final int ddb = distantDB(type);
        final Model model = getSyncModel(ddb);
        String typeStr = type.toString();
        if (type == DocType.USER_PRIVATE)
            typeStr = "user";
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        final Resource res = model.getResource(ADM+"GitSyncInfo"+typeStr);
        final Property p = model.getProperty(ADM+"hasLastRevision");
        final Statement s = model.getProperty(res, p);
        String returnvalue = null;
        if (s != null)
        	returnvalue = s.getString();
        TransferHelpers.logger.info("last revision of {} ({}) is {} in db {}", type, typeStr, returnvalue, ddb);
        return returnvalue;
    }
    
    private static volatile Model syncModel = ModelFactory.createDefaultModel();
    private static volatile boolean syncModelInitialized = false;
    private static volatile Model authSyncModel = ModelFactory.createDefaultModel();
    private static volatile boolean authSyncModelInitialized = false;
    
    public static synchronized final Model getSyncModel(final int distantDB) {
        if (distantDB == CORE) {
            if (syncModelInitialized)
                return syncModel;
            initSyncModel(distantDB);
            return syncModel;
        }
        if (authSyncModelInitialized)
            return authSyncModel;
        initSyncModel(distantDB);
        return authSyncModel;
    }
    
    public static synchronized final void initSyncModel(final int distantDB) {
        if (distantDB == CORE) {
            syncModelInitialized = true;
            logger.info("initSyncModel: {} ({})", SYSTEM_GRAPH, distantDB);
            Model distantSyncModel = getModel(SYSTEM_GRAPH, distantDB);
            if (distantSyncModel != null) {
                syncModel.add(distantSyncModel);
            } else {
                updatingFuseki = false;
            }
        } else {
            authSyncModelInitialized = true;
            logger.info("initSyncModel: {} ({})", SYSTEM_GRAPH, distantDB);
            Model distantSyncModel = getModel(SYSTEM_GRAPH, distantDB);
            if (distantSyncModel != null) {
                authSyncModel.add(distantSyncModel);
            } else {
                updatingFuseki = false;
            }
        }
    }
    
    public static synchronized void setLastRevision(final String revision, final DocType type) {
        final Model model = getSyncModel(distantDB(type));
        String typeStr = type.toString();
        if (type == DocType.USER_PRIVATE)
            typeStr = "user";
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        Resource res = model.getResource(ADM+"GitSyncInfo"+typeStr);
        Property prop = model.getProperty(ADM+"hasLastRevision");
        Literal lit = model.createLiteral(revision);
        Statement stmt = model.getProperty(res, prop);
        if (stmt == null) {
            model.add(res, prop, lit);
        } else {
            stmt.changeObject(lit);
        }
        TransferHelpers.logger.info("set last revision of {} to {}", type, revision);
        // bypass the transferModel machinery. Don't tangle adm:system w/ currentDataset
        putModel(SYSTEM_GRAPH, model, distantDB(type));
    }

    public static void setModelRevision(Model model, DocType type, String rev, String mainId) {
        if (!addGitRevision)
            return;
        final Property prop;
        if (type == DocType.ETEXTCONTENT) 
            prop = model.getProperty(ADM, "contentsGitRevision");
        else
            prop = model.getProperty(ADM, "gitRevision");
        final Resource res = model.getResource(BDA+mainId);
        res.addProperty(prop, model.createLiteral(rev));
    }
    
    public static void printUsage(String head) {
        Runtime runtime = Runtime.getRuntime();

        NumberFormat format = NumberFormat.getInstance();

        StringBuilder sb = new StringBuilder();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        sb.append("free: " + format.format(freeMemory / 1024));
        sb.append(";  allocated: " + format.format(allocatedMemory / 1024));
        sb.append(";  max: " + format.format(maxMemory / 1024));
        sb.append(";  total free: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
        
        System.err.println(head + sb.toString());
    }

    private static void loadDatasetSimple(final Dataset ds, final int distantDB) {
    	if (TransferHelpers.DRYRUN) {
    		logger.info("drymode: don't really send " + ds.getUnionModel().size() + " to Fuseki ("+distantDB+")");
    		return;
    	}
        if (distantDB == CORE) {
            if (!fuConn.isInTransaction()) {
                fuConn.begin(ReadWrite.WRITE);
            }
            fuConn.loadDataset(ds);
            logger.info("transferred ~ " + ds.getUnionModel().size() + " triples");
            fuConn.commit();
        } else {
            if (!fuAuthConn.isInTransaction()) {
                fuAuthConn.begin(ReadWrite.WRITE);
            }
            fuAuthConn.loadDataset(ds);
            logger.info("transferred ~ " + ds.getUnionModel().size() + " triples");
            fuAuthConn.commit();
        }
    }
    
    static void transferModel(final DocType docType, final String graphName, final Model m) {
        transferModel(docType, graphName, m, false);
    }
    
    static final Resource EtextInstance = ResourceFactory.createResource( TransferHelpers.CORE_PREFIX+"EtextInstance" );
    static void transferModel(final DocType docType, final String graphName, final Model model, boolean simple) {
        final int distantDB = distantDB(docType);
        if (GitToDB.ric && (TransferHelpers.isRic(model) || !TransferHelpers.isReleased(model))) {
            deleteModel(graphName, distantDB);
            if (docType == DocType.EINSTANCE) {
                ResIterator ri = model.listResourcesWithProperty(RDF.type, EtextInstance);
                if (!ri.hasNext()) {
                    logger.error("couldn't find etext instance in {}", graphName);
                } else {
                    Resource eInstance = ri.next();
                    TransferHelpers.tagAsRic(eInstance.getLocalName());
                }
            }
            return;
        }
        if (GitToDB.ric && (docType == DocType.ETEXT || docType == DocType.ETEXTCONTENT) && TransferHelpers.isInRicEInstance(model)) {
            deleteModel(graphName, distantDB);
            return;
        }
        // we don't transfer withdrawn works created from outlines:
        if (graphName.contains("/WA0XL") && TransferHelpers.isWithdrawn(model)) {
            return;
        }
        
        if (updatingFuseki) {
            putModel(graphName, model, distantDB);
            return;
        }

        int triplesInDataset = addModelToCurrentDataset(graphName, model, distantDB);
        if (simple || triplesInDataset > initialLoadBulkSize) {
            transferCurrentDataset(distantDB);
        }
    }

    private static int addModelToCurrentDataset(String graphName, Model model, int distantDB) {
        Dataset ds = getCurrentDataset(distantDB);
        ds.addNamedModel(graphName, model);
        if (distantDB == CORE) {
            triplesInDataset += model.size();
            return triplesInDataset;
        }
        triplesInAuthDataset += model.size();
        return triplesInAuthDataset;
    }

    private static void transferCurrentDataset(int distantDB) {
        if (connectPerTransfer) {
            openConnection(distantDB);
        }
        Dataset ds = getCurrentDataset(distantDB);
        loadDatasetSimple(ds, distantDB);
        if (distantDB == CORE) {
            currentDataset = null;
            triplesInDataset = 0;
        } else {
            currentAuthDataset = null;
            triplesInAuthDataset = 0;
        }
        if (connectPerTransfer) {
            closeConnection(distantDB);
            System.gc();
        }
        printUsage("USAGE  ");
    }
    
    static void finishDatasetTransfers(final int distantDB) {
        // if map is not empty, transfer the last one
        logger.info("finishDatasetTransfers addFileFuseki " + (currentDataset != null ? "not null" : "null"));
        if ((distantDB == CORE && currentDataset != null) || (distantDB == AUTH && currentAuthDataset != null)) {
            transferCurrentDataset(distantDB);
        }
        printUsage("FINISH USAGE  ");
    }

    static void deleteModel(String graphName, int distantDB) {
        logger.info("deleting " + graphName + "from Fuseki ("+distantDB+")");
        if (TransferHelpers.DRYRUN)
        	return;
        openConnection(distantDB);
        RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        if (!conn.isInTransaction()) {
            conn.begin(ReadWrite.WRITE);
        }
        try {
            conn.delete(graphName);
            conn.commit();
        } catch (HttpException e) {
            logger.warn("didn't delete graph: ", e);
        }
    }
    
    static void postModelInParts(final String graphName, final Model m, final int distantDB) {
        logger.info("post in parts, size "+m.size());
        if (m.size() <= initialLoadBulkSize) {
            putModel(graphName, m, distantDB);
        }
        int size = 0;
        deleteModel(graphName, distantDB);
        Model newModel = ModelFactory.createDefaultModel();
        final StmtIterator si = m.listStatements();
        while (si.hasNext()) {
            newModel.add(si.next());
            size += 1;
            if (size > initialLoadBulkSize) {
                postModel(graphName, newModel, distantDB);
                size = 0;
                newModel = ModelFactory.createDefaultModel();
            }
        }
        if (size > 0) {
            postModel(graphName, newModel, distantDB);
        }
    }

    static Model getModel(final String graphName, final int distantDB) {
        logger.info("getModel: {} ({})", graphName, distantDB);
        openConnection(distantDB);
        RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        try {
            Model model = conn.fetch(graphName);
            logger.info("getModel:" + graphName + "  got model: " + model.size());
            return model; 
        } catch (Exception ex) {
            logger.info("getModel:" + graphName + "  FAILED ");
            return null;
        }
    }
    
    static String getRevision(final String graphName, final int distantDB) {
        openConnection(distantDB);
        final RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        final String spquery = "select distinct ?rev { ?wad adm:graphId <"+graphName+"> ; adm:gitRevision ?rev }";
        final Query q = QueryFactory.create(spquery);
        final QueryExecution qe = conn.query(q);
        final ResultSet rs = qe.execSelect();
        String rev = null;
        if (!rs.hasNext())
            return null;
        final Literal revl = rs.next().getLiteral("?rev");
        if (revl == null)
            return null;
        rev = revl.getString();
        if (rs.hasNext())
            logger.error("graph "+graphName+" has two revisions!");
        qe.close();
        return rev;
    }

    static void getRevisions(final List<String> graphNames, final Map<String,String> ridToRevFuseki, final int distantDB) {
        openConnection(distantDB);
        final RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        final String spquery = "select distinct ?g ?rev { values ?g { <http://purl.bdrc.io/graph/"+String.join("> <http://purl.bdrc.io/graph/", graphNames)+"> } . ?wad <http://purl.bdrc.io/ontology/admin/graphId> ?g ; <http://purl.bdrc.io/ontology/admin/gitRevision> ?rev }";
        final Query q = QueryFactory.create(spquery);
        final QueryExecution qe = conn.query(q);
        final ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            final QuerySolution s = rs.next();
            if (!s.contains("?g") || !s.contains("?rev"))
                continue;
            final String g = s.getResource("?g").getURI();
            final String rev = s.getLiteral("?rev").getString();
            if (!g.startsWith("http://purl.bdrc.io/graph/"))
                continue;
            ridToRevFuseki.put(g.substring(26), rev);
        }
    }
    
    static void getRevisionsBatch(final List<String> graphNames, final Map<String,String> ridToRevFuseki, final int batchsize, final int distantDB) {
        int i = 0;
        while (i < graphNames.size()) {
            final int nbIncrement = Math.min(batchsize, graphNames.size() - i);
            getRevisions(graphNames.subList(i, i+nbIncrement), ridToRevFuseki, distantDB);
            i += nbIncrement;
        }
    }
    
    static void putModel(String graphName, Model model, final int distantDB) {
        logger.info("putting:" + graphName);
        if (GitToDB.debug)
        	model.write(System.out, "TTL");
        if (TransferHelpers.DRYRUN)
        	return;
        openConnection(distantDB);
        RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        if (!conn.isInTransaction()) {
            conn.begin(ReadWrite.WRITE);
        }
        conn.put(graphName, model);
        conn.commit();
    }
    
    static void postModel(String graphName, Model model, final int distantDB) {
        logger.info("loading:" + graphName);
        if (GitToDB.debug)
            model.write(System.out, "TTL");
        if (TransferHelpers.DRYRUN)
            return;
        openConnection(distantDB);
        RDFConnection conn = distantDB == CORE ? fuConn : fuAuthConn;
        if (!conn.isInTransaction()) {
            conn.begin(ReadWrite.WRITE);
        }
        conn.load(graphName, model);
        conn.commit();
    }
}
