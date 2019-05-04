package io.bdrc.gittodbs;

import java.net.MalformedURLException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFuseki;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class FusekiHelpers {
    
    public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
    public static String FusekiSparqlEndpoint = null;
    public static RDFConnection fuConn;
    public static int initialLoadBulkSize = 50000; // the number of triples above which a dataset load is triggered
    public static boolean addGitRevision = true;
    
    public static Logger logger = LoggerFactory.getLogger(FusekiHelpers.class);
    
    public static void init(String fusekiHost, String fusekiPort, String fusekiEndpoint) throws MalformedURLException {
        String baseUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint;
        FusekiUrl = baseUrl+"/data";
        FusekiSparqlEndpoint = baseUrl+"/query";
        logger.info("connecting to fuseki via RDFConnection at "+FusekiUrl);
        fuConn = RDFConnectionFuseki.create()
                .destination(baseUrl)
                .queryEndpoint(baseUrl+"/query")
                .gspEndpoint(baseUrl+"/data")
                .updateEndpoint(baseUrl+"/update")
                .build();
    }
    
    public static synchronized String getLastRevision(DocType type) {
        Model m = getSyncModel();
        String typeStr = type.toString();
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo"+typeStr);
        Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        Statement s = m.getProperty(res, p);
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
        logger.info("initSyncModel: " + TransferHelpers.ADMIN_PREFIX+"system");
        Model distantSyncModel = getModel(TransferHelpers.ADMIN_PREFIX+"system");
        if (distantSyncModel != null) {
            syncModel.add(distantSyncModel);
        }
    }
    
    public static synchronized void setLastRevision(String revision, DocType type) {
        final Model m = getSyncModel();
        String typeStr = type.toString();
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo"+typeStr);
        Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        Literal l = m.createLiteral(revision);
        Statement s = m.getProperty(res, p);
        if (s == null) {
            m.add(res, p, l);
        } else {
            s.changeObject(l);
        }

        transferModel(TransferHelpers.ADMIN_PREFIX+"system", m);
    }

    public static void setModelRevision(Model m, DocType type, String rev, String mainId) {
        if (!addGitRevision)
            return;
        final Property p;
        if (type == DocType.ETEXTCONTENT) 
            p = m.getProperty(TransferHelpers.ADM, "contentsGitRevision");
        else
            p = m.getProperty(TransferHelpers.ADM, "gitRevision");
        final Resource r = m.getResource(TransferHelpers.BDA+mainId);
        r.addProperty(p, m.createLiteral(rev));
    }

    private static void loadDatasetSimple(final Dataset ds) {
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        fuConn.loadDataset(ds);
        logger.info("transferred ~ " + ds.getUnionModel().size() + " triples");
        fuConn.commit();
    }

    
    static Dataset currentDataset = null;
    static int triplesInDataset = 0;
    
    static void transferModel(final String graphName, final Model m) {
        transferModel(graphName, m, false);
    }
    
    static void transferModel(final String graphName, final Model m, boolean simple) {
        if (currentDataset == null)
            currentDataset = DatasetFactory.createGeneral();
        currentDataset.addNamedModel(graphName, m);
        triplesInDataset += m.size();
        if (simple || triplesInDataset > initialLoadBulkSize) {
            loadDatasetSimple(currentDataset);
            currentDataset = null;
            triplesInDataset = 0;
        }
    }

    static void finishDatasetTransfers() {
        // if map is not empty, transfer the last one
        logger.info("finishDatasetTransfers addFileFuseki " + (currentDataset != null ? "not null" : "null"));
        if (currentDataset != null) {
            loadDatasetSimple(currentDataset);
        }
        currentDataset = null;
        triplesInDataset = 0;
    }

    public static void closeConnections() {
        logger.info("closeConnections fuConn.commit, end, close");
        FusekiHelpers.fuConn.commit();
        FusekiHelpers.fuConn.end();
        FusekiHelpers.fuConn.close();
    }

    static void deleteModel(String graphName) {
        logger.info("deleteModel:" + graphName);
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        fuConn.delete(graphName);
        fuConn.commit();
    }

    static Model getModel(String graphName) {
        logger.info("getModel:" + graphName);
        try {
            return fuConn.fetch(graphName); 
        } catch (Exception ex) {
            return null;
        }
    }
}
