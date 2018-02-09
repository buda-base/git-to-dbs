package io.bdrc.gittodbs;

import java.net.MalformedURLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class FusekiHelpers {
    
    public static String FusekiUrl = "http://localhost:13180/fuseki/bdrcrw/data";
    public static DatasetAccessor fu = null;
    public static String FusekiSparqlEndpoint = null;
    public static RDFConnection fuConn;
    public static boolean useRdfConnection = true;
    public static boolean singleModel = false;
    public static boolean serial = false;
    public static int initialLoadBulkSize = 50000; // the number of triples above which a dataset load is triggered
    public static boolean addGitRevision = true;

    protected static final String QUERY_PROLOG = 
            StrUtils.strjoinNL(
                    "prefix : <http://purl.bdrc.io/ontology/core/>",
                    "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
                    "prefix bdr: <http://purl.bdrc.io/resource/>",
                    "prefix apf: <http://jena.apache.org/ARQ/property#>"
                    );

    protected static final String PERSON_DOUBLE_QUERY = StrUtils.strjoinNL(
            QUERY_PROLOG,
            "select ?diff",
            "where {",
            "  {",
            "    select (count(?id) as ?c) (count(distinct ?id) as ?cd)",
            "    where {",
            "      ?R :personName ?b .",
            "      ?b rdfs:label ?nm .",
            "      ?b a ?type .",
            "      bind (lang(?nm) as ?lang) ",
            "      ?id apf:concat(?type '+' ?nm '@' ?lang) .",
            "    }",
            "  } .",
            "  bind (?c - ?cd as ?diff)",
            "}"
            );

    public static void init(String fusekiHost, String fusekiPort, String fusekiEndpoint) throws MalformedURLException {
        String baseUrl = "http://" + fusekiHost + ":" +  fusekiPort + "/fuseki/"+fusekiEndpoint;
        FusekiUrl = baseUrl+"/data";
        FusekiSparqlEndpoint = baseUrl+"/query";
        TransferHelpers.logger.info("connecting to fuseki on "+FusekiUrl);
        fu = DatasetAccessorFactory.createHTTP(baseUrl+"/data");
        fuConn = RDFConnectionFactory.connect(baseUrl, baseUrl+"/query", baseUrl+"/update", baseUrl+"/data");
    }
    
    public static ResultSet selectSparql(String query) {
        QueryExecution qe = QueryExecutionFactory.sparqlService(FusekiSparqlEndpoint, query);
        return qe.execSelect();
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
        Model distantSyncModel = fu.getModel(TransferHelpers.ADMIN_PREFIX+"system");
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
        try {
            transferModel(TransferHelpers.ADMIN_PREFIX+"system", m, false);
        } catch (TimeoutException e) {
            TransferHelpers.logger.warn("Timeout sending commit to fuseki (not fatal): "+revision, e);
        }
    }
    
    public static void setModelRevision(Model m, DocType type, String rev, String mainId) {
        if (!addGitRevision)
            return;
        final Property p;
        if (type == DocType.ETEXTCONTENT) 
            p = m.getProperty(TransferHelpers.ADM, "contentsGitRevision");
        else
            p = m.getProperty(TransferHelpers.ADM, "gitRevision");
        final Resource r = m.getResource(TransferHelpers.BDR+mainId);
        r.addProperty(p, m.createLiteral(rev));
    }
    
    /*
     * return true if there are doubled triples in the model; otherwise, false
     * currently only tests DocType.PERSON and returns false otherwise
     */
    public static boolean resourceDoubled(String resource, Model m, DocType type) {
        if (type == DocType.PERSON) {
            ParameterizedSparqlString qStr = new ParameterizedSparqlString(PERSON_DOUBLE_QUERY);
            qStr.setIri("?R", resource);
            Query query = QueryFactory.create(qStr.asQuery());
            try (QueryExecution qexec = QueryExecutionFactory.create(query, m)) {
                ResultSet results = qexec.execSelect() ;
                if (results.hasNext()) {
                    QuerySolution soln = results.nextSolution();
                    Literal diffLit = soln.getLiteral("diff");
                    int diff = diffLit.getInt();
                    if (diff > 0) {
                        return true;
                    }
                }
            } catch (Exception ex) {
                TransferHelpers.logger.error("checkForDoubling failed: " + ex.getMessage(), ex);
            } 
        }
            
        return false;
    }
    
    private static Model callFuseki(final String operation, final String graphName, final Model m) throws TimeoutException {
        Model res = null;
        final Callable<Model> task = new Callable<Model>() {
           public Model call() throws InterruptedException {
              switch (operation) {
              case "putModel":
                  fu.putModel(graphName, m);
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

    static private Boolean isTransfering = false;
    static ArrayBlockingQueue<Dataset> queue = new ArrayBlockingQueue<>(1);
    
    private static void loadDatasetMutex(final Dataset ds) throws TimeoutException {
        //System.out.println("loadDatasetMutex");
        // here we want the following: one transfer at a time, but while the transfer occurs, we
        // can prepare the next one.
        
        // first thing, we add the dataset to the queue, waiting for the queue to get empty fist
        // this means that we're either at the beginning of the program or that a transfer is occuring
        try {
            queue.put(ds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // if a transfer is occuring, we return, as the thread will take care of consuming the entire queue
        if (isTransfering) {
            return;
        }
        
        final Callable<Void> task = new Callable<Void>() {
           public Void call() throws InterruptedException {
               // we consume the queue
               Dataset ds = queue.poll();
               while (ds != null) {
                   if (!fuConn.isInTransaction()) {
                       fuConn.begin(ReadWrite.WRITE);
                   }
                   fuConn.loadDataset(ds);
                   System.out.println("transferred ~ " + ds.getUnionModel().size() + " triples");
                   ds = queue.poll();
               }
               fuConn.commit();
               return null;
           }
        };
        Future<Void> future = TransferHelpers.executor.submit(task);
        try {
           future.get(TransferHelpers.TRANSFER_TO, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            TransferHelpers.logger.error("interrupted during datast load", e);
        } catch (ExecutionException e) {
            TransferHelpers.logger.error("execution error during dataset load, this shouldn't happen, quitting...", e);
            System.exit(1);
        } finally {
           isTransfering = false;
           future.cancel(true); // this kills the transfer
        }
    }
    
    private static void loadDataset(final Dataset ds) throws TimeoutException {
        if (!fuConn.isInTransaction()) {
            fuConn.begin(ReadWrite.WRITE);
        }
        fuConn.loadDataset(ds);
        System.out.println("transferred ~ " + ds.getUnionModel().size() + " triples");
        fuConn.commit();
    }
    
    static Dataset currentDataset = null;
    static int triplesInDataset = 0;
    static void addToTransferBulk(final String graphName, final Model m) {
        if (currentDataset == null)
            currentDataset = DatasetFactory.createGeneral();
        currentDataset.addNamedModel(graphName, m);

        if (singleModel) {
            try {
                if (serial) {
                    loadDataset(currentDataset);
                } else {
                    loadDatasetMutex(currentDataset);
                }
                currentDataset = null;
            } catch (TimeoutException e) {
                e.printStackTrace();
                return;
            }
        } else {
            triplesInDataset += m.size();
            if (triplesInDataset > initialLoadBulkSize) {
                try {
                    if (serial) {
                        loadDataset(currentDataset);
                    } else {
                        loadDatasetMutex(currentDataset);
                    }
                    currentDataset = null;
                    triplesInDataset = 0;
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }

    static void finishDatasetTransfers() {
        // if map is not empty, transfer the last one
        if (currentDataset != null) {
            try {
                loadDatasetMutex(currentDataset);
            } catch (TimeoutException e) {
                e.printStackTrace();
                return;
            }
        }
    }
    
    public static void closeConnections() {
        if (useRdfConnection) {
            FusekiHelpers.fuConn.commit();
            FusekiHelpers.fuConn.end();
            FusekiHelpers.fuConn.close();
        }
    }
     
    static void transferModel(final String graphName, final Model m, final boolean firstTransfer) throws TimeoutException {
        if (!firstTransfer || !useRdfConnection) {
            callFuseki("putModel", graphName, m);
        } else {
            addToTransferBulk(graphName, m);
        }
    }
    
    static void deleteModel(String graphName) throws TimeoutException {
        callFuseki("deleteModel", graphName, null);
    }
    
    static Model getModel(String graphName) throws TimeoutException {
        return callFuseki("getModel", graphName, null);
    }
}
