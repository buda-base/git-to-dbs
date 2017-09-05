package io.bdrc.gittodbs;

import java.net.MalformedURLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class FusekiHelpers {
    
    public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
    public static DatasetAccessor fu = null;
    public static String FusekiSparqlEndpoint = null;
    
    public static void init(String fusekiHost, String fusekiPort, String fusekiEndpoint) throws MalformedURLException {
        FusekiUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint+"/data";
        FusekiSparqlEndpoint = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint+"/query";
        TransferHelpers.logger.info("connecting to fuseki on "+FusekiUrl);
        fu = DatasetAccessorFactory.createHTTP(FusekiUrl);
    }
    
    public static ResultSet selectSparql(String query) {
        QueryExecution qe = QueryExecutionFactory.sparqlService(FusekiSparqlEndpoint, query);
        return qe.execSelect();
    }
    
    public static String getLastRevision(DocType type) { 
        Model m = getSyncModel();
        String typeStr = TransferHelpers.typeToStr.get(type);
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo"+typeStr);
        Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        Statement s = m.getProperty(res, p);
        if (s == null) return null;
        return s.getString();
    }
    
    private static Model syncModel = null;
    
    public static synchronized Model getSyncModel() {
        if (syncModel != null) return syncModel;
        try {
            syncModel = getModel(TransferHelpers.ADMIN_PREFIX+"system");
        } catch (TimeoutException e) {
            TransferHelpers.logger.error("Time out while fetching system model, quitting...", e);
            System.exit(1);
        }
        if (syncModel == null) {
            syncModel = ModelFactory.createDefaultModel();
        }
        return syncModel;
    }
    
    public static void setLastRevision(String revision, DocType type) {
        Model m = getSyncModel();
        String typeStr = TransferHelpers.typeToStr.get(type);
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo"+typeStr);
        Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        Literal l = m.createLiteral(revision);
        Statement s = m.getProperty(res, p);
        if (s == null) {
            m.add(res, p, l);
        } else {
            s.changeObject(revision);
        }
        try {
            transferModel(TransferHelpers.ADMIN_PREFIX+"system", m);
        } catch (TimeoutException e) {
            TransferHelpers.logger.warn("Timeout sending commit to fuseki (not fatal): "+revision, e);
        }
    }
    
    private static Model callFuseki(String operation, String graphName, Model m) throws TimeoutException {
        Model res = null;
        Callable<Model> task = new Callable<Model>() {
           public Model call() throws InterruptedException {
              switch (operation) {
              case "putModel":
                  fu.putModel(graphName, m);
                  //fu.putModel(m);
                  return null;
              case "deleteModel":
                  fu.deleteModel(graphName);
                  return null;
              default:
                  return fu.getModel(graphName);
              }
           }
        };
        Future<Model> future = TransferHelpers.executor.submit(task);
        try {
           res = future.get(TransferHelpers.TRANSFER_TO, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            TransferHelpers.logger.error("interrupted during "+operation+" of "+graphName, e);
        } catch (ExecutionException e) {
            TransferHelpers.logger.error("execution error during "+operation+" of "+graphName+", this shouldn't happen, quitting...", e);
           System.exit(1);
        } finally {
           future.cancel(true); // this kills the transfer
        }
        return res;
    }
    
    static void transferModel(String graphName, Model m) throws TimeoutException {
        callFuseki("putModel", graphName, m);
    }
    
    static void deleteModel(String graphName) throws TimeoutException {
        callFuseki("deleteModel", graphName, null);
    }
    
    static Model getModel(String graphName) throws TimeoutException {
        return callFuseki("getModel", graphName, null);
    }
}
