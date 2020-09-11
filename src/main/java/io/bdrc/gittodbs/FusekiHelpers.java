package io.bdrc.gittodbs;

import java.net.MalformedURLException;
import java.text.NumberFormat;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
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
    public static String FusekiSparqlEndpoint = null;
    public static RDFConnection fuConn = null;
    public static Dataset testDataset = null;
    private static RDFConnectionRemoteBuilder fuConnBuilder = null;
    public static String baseUrl = null;
    public static int initialLoadBulkSize = 50000; // the number of triples above which a dataset load is triggered
    public static boolean addGitRevision = true;
    
    public static boolean updatingFuseki = true;
    
    public static String SYSTEM_GRAPH = BDG+"SystemGitSync";
    
    static Dataset currentDataset = null;
    static int triplesInDataset = 0;
    
    public static Logger logger = LoggerFactory.getLogger(FusekiHelpers.class);
    
    private static void initConnectionBuilder() {
        fuConnBuilder = RDFConnectionFuseki.create()
                .destination(baseUrl)
                .queryEndpoint(baseUrl+"/query")
                .gspEndpoint(baseUrl+"/data")
                .updateEndpoint(baseUrl+"/update");
    }

    public static void openConnection() {
        if (fuConn != null) {
            logger.debug("openConnection already connected to fuseki via RDFConnection at "+FusekiUrl);
            return;
        }

        logger.info("openConnection to fuseki via RDFConnection at "+FusekiUrl);
        if (testDataset != null) {
            fuConn = RDFConnectionFactory.connect(testDataset);
        } else {
            fuConn = fuConnBuilder.build();
        }
    }

    public static void closeConnection() {
        if (fuConn == null) {
            logger.info("closeConnection already closed for "+FusekiUrl);
            return;
        }

        logger.info("closeConnections fuConn.commit, end, close");
        fuConn.commit();
        fuConn.end();
        fuConn.close();
        
        fuConn = null;
    }
    
    public static void init(String fusekiHost, String fusekiPort, String fusekiEndpoint) throws MalformedURLException {
        baseUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint;
        FusekiUrl = baseUrl+"/data";
        FusekiSparqlEndpoint = baseUrl+"/query";
        initConnectionBuilder();
        openConnection();
    }
    
    public static synchronized String getLastRevision(DocType type) {
        Model model = getSyncModel();
        String typeStr = type.toString();
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        Resource res = model.getResource(ADM+"GitSyncInfo"+typeStr);
        Property p = model.getProperty(ADM+"hasLastRevision");
        Statement s = model.getProperty(res, p);
        if (s == null) return null;
        return s.getString();
    }
    
    private static volatile Model syncModel = ModelFactory.createDefaultModel();
    private static volatile boolean syncModelInitialized = false;
    
    public static synchronized final Model getSyncModel() {
        if (syncModelInitialized)
            return syncModel;
        initSyncModel();
        return syncModel;
    }
    
    public static synchronized final void initSyncModel() {
        syncModelInitialized = true;
        logger.info("initSyncModel: " + SYSTEM_GRAPH);
        Model distantSyncModel = getModel(SYSTEM_GRAPH);
        if (distantSyncModel != null) {
            syncModel.add(distantSyncModel);
        } else {
            updatingFuseki = false;
        }
    }
    
    public static synchronized void setLastRevision(String revision, DocType type) {
        final Model model = getSyncModel();
        String typeStr = type.toString();
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
        
        // bypass the transferModel machinery. Don't tangle adm:system w/ currentDataset
        putModel(SYSTEM_GRAPH, model);
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

    private static void loadDatasetSimple(final Dataset ds) {
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        fuConn.loadDataset(ds);
        logger.info("transferred ~ " + ds.getUnionModel().size() + " triples");
        fuConn.commit();
    }
    
    static void transferModel(final DocType docType, final String graphName, final Model m) {
        transferModel(docType, graphName, m, false);
    }
    
    static final Resource EtextInstance = ResourceFactory.createResource( TransferHelpers.CORE_PREFIX+"EtextInstance" );
    static void transferModel(final DocType docType, final String graphName, final Model model, boolean simple) {
        if (GitToDB.ric && TransferHelpers.isRic(model)) {
            deleteModel(graphName);
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
            deleteModel(graphName);
            return;
        }
        
        if (updatingFuseki) {
            putModel(graphName, model);
            return;
        }

        if (currentDataset == null)
            currentDataset = DatasetFactory.createGeneral();
        currentDataset.addNamedModel(graphName, model);
        triplesInDataset += model.size();
        if (simple || triplesInDataset > initialLoadBulkSize) {
            if (connectPerTransfer) {
                openConnection();
            }
            loadDatasetSimple(currentDataset);
            currentDataset = null;
            triplesInDataset = 0;
            if (connectPerTransfer) {
                closeConnection();
                System.gc();
            }
            printUsage("USAGE  ");
        }
    }

    static void finishDatasetTransfers() {
        // if map is not empty, transfer the last one
        logger.info("finishDatasetTransfers addFileFuseki " + (currentDataset != null ? "not null" : "null"));
        if (currentDataset != null) {
            if (connectPerTransfer) {
                openConnection();
            }
            loadDatasetSimple(currentDataset);
        }
        currentDataset = null;
        triplesInDataset = 0;
        if (connectPerTransfer) {
            closeConnection();
            System.gc();
        }
        printUsage("FINISH USAGE  ");
    }

    static void deleteModel(String graphName) {
        logger.info("DELETING:" + graphName);
        openConnection();
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        try {
            fuConn.delete(graphName);
            fuConn.commit();
        } catch (HttpException e) {
            logger.warn("didn't delete graph: ", e);
        }
    }

    static Model getModel(String graphName) {
        logger.info("getModel:" + graphName);
        try {
            openConnection();
            Model model = fuConn.fetch(graphName);
            logger.info("getModel:" + graphName + "  got model: " + model.size());
            return model; 
        } catch (Exception ex) {
            logger.info("getModel:" + graphName + "  FAILED ");
            return null;
        }
    }

    static void putModel(String graphName, Model model) {
        logger.info("PUTTING:" + graphName);
        openConnection();
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        fuConn.put(graphName, model);
        fuConn.commit();
    }
}
