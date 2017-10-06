package io.bdrc.gittodbs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DbAccessException;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class CouchHelpers {
    
    public static Hashtable<DocType, CouchDbConnector> dbs = new Hashtable<>();
    public static CouchDbInstance dbInstance;
    public static HttpClient httpClient;
    public static String url = "http://localhost:13598";
    public static boolean deleteDbBeforeInsert = true;
    public static final String CouchDBPrefixGen = "bdrc_";
    public static final String CouchDBPrefixLib = "lib_";
    public static String CouchDBPrefix = CouchDBPrefixGen;
    public static final String GitRevDoc = "gitSync";
    public static final String GitRevField = "adm:gitRev"; // cannot start with '_'...
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
    private static final long bulkSizeTriples = 20000;
    private static final long bulkSizeDocs = 100;
    private static boolean useBulks = true;
    // test mode indicates if we're using mcouch or not. This matters because
    // mcouch doesn't support the show function of design documents, but we want
    // to use it in production.
    public static boolean testMode = false;
    
    public static void init(String couchDBHost, String couchDBPort, boolean libFormat) {
        url = "http://" + couchDBHost + ":" +  couchDBPort;
        TransferHelpers.logger.info("connecting to CouchDB on "+url);
        try {
            httpClient = new StdHttpClient.Builder()
                    .url(url)
                    .build();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        dbInstance = new StdCouchDbInstance(httpClient);
        if (libFormat)
            CouchDBPrefix = CouchDBPrefixLib;
        putDBs(libFormat);
    }
    
    public static void putDBs(boolean libFormat) {
        putDB(DocType.ITEM);
        putDB(DocType.WORK);
        putDB(DocType.PERSON);
        if (!libFormat) {
            putDB(DocType.CORPORATION);
            putDB(DocType.LINEAGE);
            putDB(DocType.OFFICE);
            putDB(DocType.PLACE);
            putDB(DocType.PRODUCT);
            putDB(DocType.TOPIC);
        }
    }
    
    public static final Map<DocType,Boolean> wasEmpty = new EnumMap<>(DocType.class);
    
    public static void putDB(DocType type) {
        String DBName = CouchDBPrefix+TransferHelpers.typeToStr.get(type);
        boolean justCreatedDatabase = deleteDbBeforeInsert;
        if (deleteDbBeforeInsert)
            dbInstance.deleteDatabase(DBName);
        if (deleteDbBeforeInsert || !dbInstance.checkIfDbExists(DBName)) {
            dbInstance.createDatabase(DBName); 
            justCreatedDatabase = true;
            wasEmpty.put(type, true);
            System.out.println(DBName+" empty");
        }
        //TransferHelpers.logger.info("connecting to database "+DBName);
        System.out.println("connecting to database "+DBName);
        CouchDbConnector db = new StdCouchDbConnector(DBName, dbInstance);
        ClassLoader classLoader = CouchHelpers.class.getClassLoader();
        if (justCreatedDatabase) {
            InputStream inputStream = classLoader.getResourceAsStream("design-jsonld.json");
            Map<String, Object> jsonMap;
            try {
                jsonMap = objectMapper.readValue(inputStream, typeRef);
                db.create(jsonMap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        dbs.put(type, db);
    }
    
    public static String inputStreamToString(InputStream s) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        try {
            while ((length = s.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
        } catch (IOException e1) {
            e1.printStackTrace();
            return null;
        }
        try {
            return result.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static String getRevision(String documentName, DocType type) {
        CouchDbConnector db = dbs.get(type);
        if (db == null) {
            System.err.println("cannot get couch connector for type "+type);
            return null;
        }
        if (testMode) {
            final InputStream oldDocStream = db.getAsStream(documentName);
            Map<String, Object> doc;
            try {
                doc = objectMapper.readValue(oldDocStream, typeRef);
            } catch (IOException e) {
                TransferHelpers.logger.error("This really shouldn't happen!", e);
                return null;
            }
            if (doc != null) {
                return doc.get("_rev").toString();
            }
            return null;
        }
        final String uri = "/"+CouchDBPrefix+TransferHelpers.typeToStr.get(type)+"/_design/jsonld/_show/revOnly/" + documentName;
        final HttpResponse r = db.getConnection().get(uri);
        final InputStream stuff = r.getContent();
        final String result = inputStreamToString(stuff);
        if (result.charAt(0) == '{') {
            return null;
        }
        return result.substring(1, result.length()-1);
    }
    
    static private Boolean isTransfering = false;
    static ArrayBlockingQueue<List<Object>> queue = new ArrayBlockingQueue<>(1);
    static LinkedList<DocType> typeQueue = new LinkedList<>();
    
    private static void loadBulkMutex(final List<Object> bulk, final DocType type) throws TimeoutException {
        // see comments in FusekiHelpers. The code is similar and should probably be merged 
        try {
            queue.put(bulk);
            typeQueue.add(type);
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
               List<Object> bulk = queue.poll();
               DocType type = typeQueue.pop();
               while (bulk != null) {
                   final CouchDbConnector db = dbs.get(type);
                   System.out.print("starting to transfer "+bulk.size()+" "+TransferHelpers.typeToStr.get(type)+" documents...");
                   db.executeBulk(bulk);
                   System.out.println("done");
                   bulk = queue.poll();
                   if (bulk != null)
                       type = typeQueue.pop();
               }
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
    
    static List<Object> currentBulk = null;
    static DocType currentType = null;
    static long triplesInBulk = 0;
    static long docsInBulk = 0;
    static void addToTransferBulk(final Object o, final long sizeHintTriples, final DocType type) {
        if (currentType != type && currentBulk != null) {
            try {
                loadBulkMutex(currentBulk, currentType);
                currentBulk = null;
                triplesInBulk = 0;
                docsInBulk = 0;
            } catch (TimeoutException e) {
                e.printStackTrace();
                return;
            }
        }
        
        currentType = type;
        
        if (currentBulk == null)
            currentBulk = new ArrayList<Object>();
        
        currentBulk.add(o);
        triplesInBulk += sizeHintTriples;
        docsInBulk += 1;
        if (triplesInBulk >= bulkSizeTriples || docsInBulk >= bulkSizeDocs) {
            try {
                loadBulkMutex(currentBulk, type);
                currentBulk = null;
                triplesInBulk = 0;
                docsInBulk = 0;
            } catch (TimeoutException e) {
                e.printStackTrace();
                return;
            }
        }
    }
    
    static void finishBulkTransfers() {
        // if map is not empty, transfer the last one
        if (currentBulk != null) {
            try {
                loadBulkMutex(currentBulk, currentType);
            } catch (TimeoutException e) {
                e.printStackTrace();
                return;
            }
        }
    }
    
    public static void couchUpdateOrCreate(final Map<String,Object> jsonObject, final String documentName, final DocType type, final String commitRev, final long sizeHintTriples) {
        final CouchDbConnector db = dbs.get(type);
        if (db == null) {
            System.err.println("cannot get couch connector for type "+type);
            return;
        }
        jsonObject.put("_id", documentName);
        jsonObject.put(GitRevField, commitRev);
        if (wasEmpty.containsKey(type)) {
            // if we start from an empty base, no need to check for an old revision for a document
            if (useBulks) {
                addToTransferBulk(jsonObject, sizeHintTriples, type);
            } else {
                db.create(jsonObject);
            }
            return;
        }
        try {
            final String oldRev = getRevision(documentName, type);
            if (oldRev == null) {
                if (useBulks) {
                    addToTransferBulk(jsonObject, sizeHintTriples, type);
                } else {
                    db.create(jsonObject);
                }
            } else {
                jsonObject.put("_rev", oldRev);
                db.update(jsonObject);
            }
        } catch (DocumentNotFoundException e) {
            if (useBulks) {
                addToTransferBulk(jsonObject, sizeHintTriples, type);
            } else {
                db.create(jsonObject);
            }
        }
    }
    
    public static void couchDelete(final String mainId, final DocType type) {
        final CouchDbConnector db = dbs.get(type);
        if (db == null) {
            System.err.println("cannot get couch connector for type "+type);
            return;
        }
        final String documentName = "bdr:"+mainId;
        try {
            final String oldRev = getRevision(documentName, type);
            if (oldRev != null)
                db.delete(documentName, oldRev);
        } catch (DocumentNotFoundException e) { }
    }
    
    public static void jsonObjectToCouch(Map<String,Object> jsonObject, String mainId, DocType type, String commitRev, long sizeHint) {
        String documentName = "bdr:"+mainId;
        couchUpdateOrCreate(jsonObject, documentName, type, commitRev, sizeHint);
    }

    public static synchronized Model getSyncModel(DocType type) {
        return getModelFromDocId(GitRevDoc, type);
    }
    
    public static Model getModelFromDocId(String docId, DocType type) {
        final CouchDbConnector db = dbs.get(type);
        final Model res = ModelFactory.createDefaultModel();
        if (testMode) {
            final InputStream oldDocStream;
            try {
                oldDocStream = db.getAsStream(docId);
            } catch (DocumentNotFoundException e) {
                return null;
            }
            Map<String, Object> doc;
            try {
                doc = objectMapper.readValue(oldDocStream, typeRef);
            } catch (IOException e) {
                TransferHelpers.logger.error("This really shouldn't happen!", e);
                return null;
            }
            doc.remove("_id");
            doc.remove("_rev");
            doc.remove(GitRevField);
            String docstring;
            try {
                docstring = objectMapper.writeValueAsString(doc);
            } catch (JsonProcessingException e) {
                TransferHelpers.logger.error("This really shouldn't happen!", e);
                return null;
            }
            StringReader docreader = new StringReader(docstring);
            res.read(docreader, "", "JSON-LD");
            return res;
        }
        // https://github.com/helun/Ektorp/issues/263
        final String uri = "/"+CouchDBPrefix+TransferHelpers.typeToStr.get(type)+"/_design/jsonld/_show/jsonld/" + docId;
        final HttpResponse r = db.getConnection().get(uri);
        final InputStream stuff = r.getContent();
        // feeding the inputstream directly to jena for json-ld parsing
        // instead of ektorp json parsing
        final StreamRDF dest = StreamRDFLib.graph(res.getGraph());
        final RDFParserBuilder rdfp = RDFParser.source(stuff).lang(Lang.JSONLD);
        rdfp.parse(dest);
        try {
            stuff.close();
        } catch (IOException e) {
            TransferHelpers.logger.error("This really shouldn't happen!", e);
            return null;
        }
        return res;
    }
    
    public static void setLastRevision(final String revision, final DocType type) {
        Model m = getSyncModel(type);
        if (m == null)
            m = ModelFactory.createDefaultModel();
        final Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo");
        final Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        final Literal l = m.createLiteral(revision);
        final Statement s = m.getProperty(res, p);
        if (s == null) {
            m.add(res, p, l);
        } else {
            s.changeObject(revision);
        }
        final Map<String,Object> syncJson = JSONLDFormatter.modelToJsonObject(m, type, null, RDFFormat.JSONLD_COMPACT_PRETTY);
        couchUpdateOrCreate(syncJson, GitRevDoc, type, revision, 10);
    }

    public static String getLastRevision(final DocType type) {
        Model m = getSyncModel(type);
        if (m == null)
            m = ModelFactory.createDefaultModel();
        String typeStr = TransferHelpers.typeToStr.get(type);
        typeStr = typeStr.substring(0, 1).toUpperCase() + typeStr.substring(1);
        final Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo"+typeStr);
        final Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        final Statement s = m.getProperty(res, p);
        if (s == null) return null;
        return s.getString();
    }
}
