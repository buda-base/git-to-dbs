package io.bdrc.gittodbs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Hashtable;
import java.util.Map;

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
import org.ektorp.DocumentNotFoundException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class CouchHelpers {
    
    public static Hashtable<DocType, CouchDbConnector> dbs = new Hashtable<>();
    public static CouchDbInstance dbInstance;
    public static HttpClient httpClient;
    public static String url = "http://localhost:13598";
    public static boolean deleteDbBeforeInsert = false;
    public static final String CouchDBPrefix = "bdrc_";
    public static final String GitRevDoc = "_gitSync";
    
    public static Map<DocType,String> typeToName = new EnumMap<>(DocType.class);
    static {
        typeToName.put(DocType.PERSON, "person");
        typeToName.put(DocType.WORK, "work");
        typeToName.put(DocType.PLACE, "place");
        typeToName.put(DocType.TOPIC, "topic");
        typeToName.put(DocType.LINEAGE, "lineage");
        typeToName.put(DocType.CORPORATION, "corporation");
        typeToName.put(DocType.PRODUCT, "product");
        typeToName.put(DocType.ITEM, "itemImageAsset"); // TODO: possible problem here
        typeToName.put(DocType.OFFICE, "role");
    }
    
    public static void init(String couchDBHost, String couchDBPort) {
        url = "http://" + couchDBHost + ":" +  couchDBPort;
        try {
            TransferHelpers.logger.info("connecting to CouchDB on "+url);
            httpClient = new StdHttpClient.Builder()
                    .url(url)
                    .build();
            dbInstance = new StdCouchDbInstance(httpClient);

            putDB(DocType.CORPORATION);
            putDB(DocType.LINEAGE);
            putDB(DocType.OFFICE);
            putDB(DocType.PERSON);
            putDB(DocType.PLACE);
            putDB(DocType.PRODUCT);
            putDB(DocType.TOPIC);
            putDB(DocType.ITEM);
            putDB(DocType.WORK);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    
    public static void putDB(DocType type) {
        String DBName = CouchDBPrefix+typeToName.get(type);
        if (deleteDbBeforeInsert) {
            dbInstance.deleteDatabase(DBName);
            dbInstance.createDatabase(DBName);
        }
        CouchDbConnector db = new StdCouchDbConnector(DBName, dbInstance);
        ClassLoader classLoader = CouchHelpers.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("design-jsonld.json");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap;
        try {
            jsonMap = mapper.readValue(inputStream, Map.class);
            db.create(jsonMap);
        } catch (IOException e) {
            e.printStackTrace();
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
    
    public static void couchUpdateOrCreate(Map<String,Object> jsonObject, String documentName, DocType type, String commitRev) {
        CouchDbConnector db = dbs.get(type);
        if (db == null) {
            System.err.println("cannot get couch connector for type "+type);
            return;
        }
        jsonObject.put("_id", documentName);
        jsonObject.put("_gitRev", documentName);
        try {
            String uri = CouchDBPrefix+type+"/_design/jsonld/_show/revOnly/" + documentName;
            HttpResponse r = db.getConnection().get(uri);
            InputStream stuff = r.getContent();
            String result = inputStreamToString(stuff);
            if (result.charAt(0) == '{') {
                db.create(jsonObject);
                return;
            }
            result = result.substring(1, result.length()-1);
            jsonObject.put("_rev", result);
            db.update(jsonObject);
        } catch (DocumentNotFoundException e) {
            db.create(jsonObject);
        }
    }
    
    public static void jsonObjectToCouch(Map<String,Object> jsonObject, String mainId, DocType type, String commitRev) {
        String documentName = "bdr:"+mainId;
        couchUpdateOrCreate(jsonObject, documentName, type, commitRev);
    }
    

    public static synchronized Model getSyncModel(DocType type) {
        return getModelFromDocId(GitRevDoc, type);
    }
    
    public static Model getModelFromDocId(String docId, DocType type) {
        CouchDbConnector db = dbs.get(type);
        Model res = ModelFactory.createDefaultModel();
        // https://github.com/helun/Ektorp/issues/263
        String uri = "/"+typeToName.get(type)+"/_design/jsonld/_show/jsonld/" + docId;
        HttpResponse r = db.getConnection().get(uri);
        InputStream stuff = r.getContent();
        // feeding the inputstream directly to jena for json-ld parsing
        // instead of ektorp json parsing
        StreamRDF dest = StreamRDFLib.graph(res.getGraph());
        RDFParserBuilder rdfp = RDFParser.source(stuff).lang(Lang.JSONLD);
        rdfp.parse(dest);
        try {
            stuff.close();
        } catch (IOException e) {
            TransferHelpers.logger.error("This really shouldn't happen!", e);
        }
        return res;
    }
    
    public static void setLastRevision(String revision, DocType type) {
        Model m = getSyncModel(type);
        Resource res = m.getResource(TransferHelpers.ADMIN_PREFIX+"GitSyncInfo");
        Property p = m.getProperty(TransferHelpers.ADMIN_PREFIX+"hasLastRevision");
        Literal l = m.createLiteral(revision);
        Statement s = m.getProperty(res, p);
        if (s == null) {
            m.add(res, p, l);
        } else {
            s.changeObject(revision);
        }
        Map<String,Object> syncJson = JSONLDFormatter.modelToJsonObject(m, type, null, RDFFormat.JSONLD_COMPACT_PRETTY);
        couchUpdateOrCreate(syncJson, GitRevDoc, type, revision);
    }
}
