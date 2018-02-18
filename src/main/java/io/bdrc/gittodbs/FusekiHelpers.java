package io.bdrc.gittodbs;

import java.net.MalformedURLException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

public class FusekiHelpers {
    
    public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
    public static String FusekiSparqlEndpoint = null;
    public static RDFConnection fuConn;
    public static int initialLoadBulkSize = 50000; // the number of triples above which a dataset load is triggered
    
    public static void init(String fusekiHost, String fusekiPort, String fusekiEndpoint) throws MalformedURLException {
        String baseUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint;
        FusekiUrl = baseUrl+"/data";
        FusekiSparqlEndpoint = baseUrl+"/query";
        TransferHelpers.logger.info("connecting to fuseki via RDFConnection at "+FusekiUrl);
        fuConn = RDFConnectionFactory.connect(baseUrl, baseUrl+"/query", baseUrl+"/update", baseUrl+"/data");
    }
    
    private static volatile Model syncModel = ModelFactory.createDefaultModel();
    private static volatile boolean syncModelInitialized = false;
    
    public static final Model getSyncModel() {
        if (syncModelInitialized)
            return syncModel;
        initSyncModel();
        return syncModel;
    }
    
    public static final void initSyncModel() {
        syncModelInitialized = true;
        TransferHelpers.logger.info("initSyncModel: " + TransferHelpers.ADMIN_PREFIX+"system");
        Model distantSyncModel = getModel(TransferHelpers.ADMIN_PREFIX+"system");
        if (distantSyncModel != null) {
            syncModel.add(distantSyncModel);
        }
    }

    private static void loadDatasetSimple(final Dataset ds) {
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        TransferHelpers.logger.info("transferring ~ " + ds.getUnionModel().toString().substring(0, 512));
        fuConn.loadDataset(ds);
        TransferHelpers.logger.info("transferred ~ " + ds.getUnionModel().size() + " triples");
        fuConn.commit();
    }

    
    static Dataset currentDataset = null;
    static int triplesInDataset = 0;
    
    static void transferModel(final String graphName, final Model m) {
        if (currentDataset == null)
            currentDataset = DatasetFactory.createGeneral();
        currentDataset.addNamedModel(graphName, m);
        triplesInDataset += m.size();
        if (triplesInDataset > initialLoadBulkSize) {
            loadDatasetSimple(currentDataset);
            currentDataset = null;
            triplesInDataset = 0;
        }
    }

    static void finishDatasetTransfers() {
        // if map is not empty, transfer the last one
        TransferHelpers.logger.info("finishDatasetTransfers addFileFuseki currentDataset " + (currentDataset != null ? "not null" : "null"));
        if (currentDataset != null) {
            loadDatasetSimple(currentDataset);
            currentDataset = null;
            triplesInDataset = 0;
        }
    }

    public static void closeConnections() {
        TransferHelpers.logger.info("closeConnections fuConn.commit, end, close");
        FusekiHelpers.fuConn.commit();
        FusekiHelpers.fuConn.end();
        FusekiHelpers.fuConn.close();
    }

    static Model getModel(String graphName) {
        TransferHelpers.logger.info("getModel:" + graphName);
        try {
            return fuConn.fetch(graphName); 
        } catch (Exception ex) {
            return null;
        }
    }
}
