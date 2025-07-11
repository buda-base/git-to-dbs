package io.bdrc.gittodbs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.bdrc.ewtsconverter.EwtsConverter;
import io.bdrc.ewtsconverter.TransConverter;
import io.bdrc.gittodbs.TransferHelpers.DocType;
import io.bdrc.libraries.Models;

public class ESUtils {
    
    public static final Logger logger = LoggerFactory.getLogger(ESUtils.class);
    public static String jsonfolder = "json/";
    public static final boolean todisk = false;
    public static String indexName = "bdrc_prod";
    
    final static EwtsConverter ewtsc = new EwtsConverter();
    
    final static String TMP = "http://purl.bdrc.io/ontology/tmp/";
    
    // PT is property type
    final static int PT_DIRECT = 1; // simple direct property with a literal as object
    final static int PT_LABEL_ONT = 2; // property where the label of the object is in the ontology
    final static int PT_MERGE = 3; // property where the object's model should be merged (case of wa, w, ie)
    final static int PT_LABEL_EXT = 4; // property where the label of the object is in the graph of the object
    final static int PT_IGNORE = 5; // property to ignore
    final static int PT_RES_ONLY = 8; // only record object rid in the associated resources array
    final static int PT_RES_ANCESTORS = 9; // only record object rid in the associated resources array but also record ancestors
    final static int PT_TYPED_LABEL = 6; // property like hasTitle or personName
    final static int PT_SPECIAL = 7; // property like hasTitle or personName
    final static int PT_NESTED = 10; // property like hasTitle or personName
    
    final static class PropInfo {
        int pt;
        String key_base = null;
        Property subProp = null;
        boolean addToAssociated = false;
        
        PropInfo(final int pt, final String key_base, final Property subProp) {
            this.pt = pt;
            this.key_base = key_base;
            this.subProp = subProp;
        }
        
        PropInfo(final int pt, final String key_base, final Property subProp, final boolean addToAssociated) {
            this.pt = pt;
            this.key_base = key_base;
            this.subProp = subProp;
            this.addToAssociated = addToAssociated;
        }
    }
    
    static float transform_score(final int score, final int minScore, final int maxScore, final float[] range) {
        float step1 = (score - minScore) / (float) maxScore;
        float multiplier = range[1] - range[0];
        return range[0] + (multiplier * step1);
    }
    
    
    // copied from editserv, should probably be moved to libraries
    public static final List<String> entity_prefix_3 = Arrays.asList("WAS", "ITW", "PRA");
    public static final List<String> entity_prefix_2 = Arrays.asList("WA", "MW", "PR", "IE", "UT", "IT", "VE");
    public static final List<String> entity_prefix_1 = Arrays.asList("W", "P", "G", "R", "L", "C", "T", "I", "U", "V", "O");
    public static final List<String> entitySubs = Arrays.asList("I", "UT", "V", "VE");
    
    public static String getTypePrefix(final String lname) {
        if (lname.isEmpty()) return null;
        if (lname.length() >= 3 && entity_prefix_3.contains(lname.substring(0,3)))
            return lname.substring(0,3);
        if (lname.length() >= 2 && entity_prefix_2.contains(lname.substring(0,2)))
            return lname.substring(0,2);
        if (entity_prefix_1.contains(lname.substring(0,1)))
            return lname.substring(0,1);
        return null;
    }
    
    static Map<String,Float[]> getScores(final String fname, final boolean scoreFirst, final float[] range) {
        Integer maxScore = 0;
        Integer minScore = -1;
        final Map<String,Float[]> res = new HashMap<>();
        final Map<String,Integer> lnameToScore = new HashMap<>();
        final Map<String,Integer[]> prefixToMinMax = new HashMap<>();
        BufferedReader reader = null;
        final File f = new File(fname);
        if (f.isFile()) {
            try {
                reader = new BufferedReader(new FileReader(f));
            } catch (FileNotFoundException e) { } // very stupid
        } else {
            final InputStream is = ESUtils.class.getResourceAsStream(fname);
            if (is == null) {
                logger.error("cannot open "+fname);
                return res;
            }
            reader = new BufferedReader(new InputStreamReader(is));
        }
        try {
            while(reader.ready()) {
                final String line = reader.readLine();
                final String[] linecomponents = line.split(",");
                if (linecomponents.length < 2)
                    continue;
                final String scoreStr = scoreFirst ? linecomponents[0] : linecomponents[1];
                final String lname = scoreFirst ? linecomponents[1] : linecomponents[0];
                final String prefix = getTypePrefix(lname);
                if (!prefixToMinMax.containsKey(prefix))
                    prefixToMinMax.put(prefix, new Integer[] {0,-1});
                Integer[] prefixMinMax = prefixToMinMax.get(prefix);
                final Integer score = Integer.valueOf(scoreStr);
                maxScore = Math.max(maxScore, score);
                minScore = minScore == -1 ? score : Math.min(score,  minScore);
                prefixMinMax[0] = prefixMinMax[0] == -1 ? score : Math.min(score,  prefixMinMax[0]);
                prefixMinMax[1] = Math.max(score,  prefixMinMax[1]);
                lnameToScore.put(lname, score);
            }
        } catch (IOException e) {
            logger.error("error reading "+fname, e);
        }
        for (Map.Entry<String,Integer> e : lnameToScore.entrySet()) {
            final String prefix = getTypePrefix(e.getKey());
            final Integer[] prefixMinMax = prefixToMinMax.get(prefix);
            final float scoreInPrefix = transform_score(e.getValue(), prefixMinMax[0], prefixMinMax[1], range);
            final float globalScore = transform_score(e.getValue(), prefixMinMax[0], prefixMinMax[1], range);
            final Float[] resForLname = new Float[2];
            resForLname[0] = globalScore;
            resForLname[1] = scoreInPrefix;
            res.put(e.getKey(), resForLname);
        }
        return res;
    }
    
    final static Map<String,Float[]> user_popularity_scores = getScores("user_popularity.csv", true, new float[] {(float) 0.4, (float) 1.0});
    final static Map<String,Float[]> entity_scores = getScores("entityScores.csv", false, new float[] {(float) 0.5, (float) 1.0});
    
    static void add_scores(final String lname, final ObjectNode doc) {
        final Float[] userpopscore = user_popularity_scores.get(lname);
        if (userpopscore != null && !lname.startsWith("WA")) {
            final JsonNode initialuserpopscoreN = doc.get("pop_score");
            if (initialuserpopscoreN == null) {
                doc.put("pop_score", userpopscore[0]);
                doc.put("pop_score_rk", userpopscore[0]);
                doc.put("pop_score_in_type", userpopscore[1]);
            } else {
                doc.put("pop_score", Math.max(userpopscore[0], doc.get("pop_score").asDouble()));
                doc.put("pop_score_rk", Math.max(userpopscore[0], doc.get("pop_score_rk").asDouble()));
                doc.put("pop_score_in_type", Math.max(userpopscore[1], doc.get("pop_score").asDouble()));
            }
        }
        // an absent rank feature will result in the results not being displayed in rank_feature queries
        if (userpopscore == null && !lname.startsWith("WA")) {
            final JsonNode initialuserpopscoreN = doc.get("pop_score_rk");
            if (initialuserpopscoreN == null)
                doc.put("pop_score_rk", 0.1);
        }
        final Float[] entityscore = entity_scores.get(lname);
        if (entityscore != null) {
            final JsonNode initialscoreN = doc.get("db_score");
            if (initialscoreN == null) {
                doc.put("db_score", entityscore[0]);
                doc.put("db_score_in_type", entityscore[1]);
            }
        }
    }
    
    private static final Map<Property, PropInfo> propInfoMap = new HashMap<>();
    static {
        // General properties
        propInfoMap.put(RDF.type, new PropInfo(PT_RES_ONLY, "type", null));
        propInfoMap.put(SKOS.prefLabel, new PropInfo(PT_DIRECT, "prefLabel", null));
        propInfoMap.put(RDFS.label, new PropInfo(PT_DIRECT, "prefLabel", null));
        propInfoMap.put(SKOS.altLabel, new PropInfo(PT_DIRECT, "altLabel", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "inCollection"), new PropInfo(PT_RES_ONLY, "inCollection", null, true));
        propInfoMap.put(SKOS.definition, new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(RDFS.comment, new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "note"), new PropInfo(PT_SPECIAL, "comment", ResourceFactory.createProperty(Models.BDO, "noteText")));

        // Person properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "associatedTradition"), new PropInfo(PT_RES_ONLY, "associatedTradition", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "personGender"), new PropInfo(PT_RES_ONLY, "personGender", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "personName"), new PropInfo(PT_SPECIAL, "altLabel", RDFS.label));
        
        // MW properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BF, "identifiedBy"), new PropInfo(PT_SPECIAL, "other_id", RDF.value));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "instanceOf"), new PropInfo(PT_MERGE, null, null, true));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "serialInstanceOf"), new PropInfo(PT_LABEL_EXT, "seriesName", null, true));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "seriesNumber"), new PropInfo(PT_DIRECT, "issueName", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "instanceHasReproduction"), new PropInfo(PT_MERGE, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "authorshipStatement"), new PropInfo(PT_DIRECT, "authorshipStatement", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "biblioNote"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "hasSourcePrintery"), new PropInfo(PT_RES_ONLY, "hasSourcePrintery", null, true));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "hasTitle"), new PropInfo(PT_SPECIAL, "altLabel", RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "printMethod"), new PropInfo(PT_RES_ONLY, "printMethod", RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "script"), new PropInfo(PT_RES_ONLY, "script", RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "publisherName"), new PropInfo(PT_DIRECT, "publisherName", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "incipit"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "colophon"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "publisherLocation"), new PropInfo(PT_DIRECT, "publisherLocation", null));
        
        // W / IE properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "scanInfo"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "etextInfo"), new PropInfo(PT_DIRECT, "comment", null));
        
        // WA properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "catalogInfo"), new PropInfo(PT_DIRECT, "summary", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "language"), new PropInfo(PT_RES_ONLY, "language", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "workIsAbout"), new PropInfo(PT_RES_ONLY, "workIsAbout", null, true));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "workGenre"), new PropInfo(PT_RES_ONLY, "workGenre", null, true));
        
        // MW in outlines properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "inRootInstance"), new PropInfo(PT_RES_ONLY, "inRootInstance", null));
        //propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "partOf"), new PropInfo(PT_RES_ONLY, "partOf", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "partType"), new PropInfo(PT_RES_ONLY, "type", null));
        
        // G properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "placeLocatedIn"), new PropInfo(PT_RES_ONLY, "locatedIn", null, true));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "placeType"), new PropInfo(PT_RES_ONLY, "placeType", null));
    }
    
    static final ObjectMapper om = new ObjectMapper();
    
    // to get the creator's cache:
    // select ?p ?pl where {
    //   ?p a :Person .
    //   FILTER(exists{ ?wac :agent ?p . })
    //   ?p skos:prefLabel ?pl .
    // }
    
    // to get the works to import:
    // select distinct ?wa ?l {
     //     ?wa2 :workIsAbout ?wa .
     //      ?wa a :Work .
     //     FILTER(not exists {?wa :workHasInstance ?mw })
     //     FILTER(exists {?wadm adm:adminAbout ?wa ; adm:status bda:StatusReleased . })
     //   }
    
    static final String waListQuery = "select distinct ?wa {\n"
            + "  {\n"
            + "    ?wa2 :workIsAbout ?wa .\n"
            + "    ?wa a :Work .\n"
            + "  } union {\n"
            + "    ?wa :workHasParallelsIn ?wa2 . \n"
            + "  }\n"
            + "  FILTER(not exists {?wa :workHasInstance ?mw })\n"
            + "  FILTER(exists {?wadm adm:adminAbout ?wa ; adm:status bda:StatusReleased . })\n"
            + "}";
    
    static Map<String,Boolean> worksToSend = null;
    
    static final void add_to_works_cache_fun(final QuerySolution qs, final Map<String,Boolean> cache) {
        final String walname = qs.getResource("?wa").getLocalName();
        cache.put(walname, true);
    }
    
    static final Map<String,Boolean> getWorksToSend() {
        if (worksToSend == null)
            return worksToSend;
        FusekiHelpers.openConnection(0);
        final RDFConnection conn = FusekiHelpers.fuConn;
        worksToSend = new HashMap<>();
        final Consumer<QuerySolution> add_to_cache = x -> add_to_works_cache_fun(x, worksToSend);
        try {
            conn.querySelect(waListQuery, add_to_cache);
        } catch (Exception ex) {
            logger.error("cannot run "+waListQuery, ex);
            return null;
        }
        return worksToSend;
    }
    
    static Map<String,List<String[]>> creatorsLabelCache = null;
    
    static final void add_to_cache_fun(final QuerySolution qs, final Map<String,List<String[]>> cache) {
        final String plname = qs.getResource("?p").getLocalName();
        final Literal pl = qs.getLiteral("?pl");
        final String[] norm = normalize_lit(pl);
        if (!cache.containsKey(plname))
            cache.put(plname, new ArrayList<>());
        cache.get(plname).add(norm);
    }
    
    static Map<String,List<String[]>> getCreatorsLabelCache() {
        if (creatorsLabelCache != null)
            return creatorsLabelCache;
        final String queryStr = "select ?p ?pl where { ?p a <"+Models.BDO+"Person> . FILTER(exists{ ?wac <"+Models.BDO+"agent> ?p . }) ?p <"+SKOS.uri+"prefLabel> ?pl . }";
        FusekiHelpers.openConnection(0);
        final RDFConnection conn = FusekiHelpers.fuConn;
        creatorsLabelCache = new HashMap<>();
        final Consumer<QuerySolution> add_to_cache = x -> add_to_cache_fun(x, creatorsLabelCache);
        try {
            conn.querySelect(queryStr, add_to_cache);
        } catch (Exception ex) {
            logger.error("cannot run "+queryStr, ex);
            return null;
        }
        return creatorsLabelCache;
    }
    
    final static class EtextInfo {
        final Integer access;
        final Float quality;
        
        EtextInfo(final Integer access, final Float quality) {
            this.access = access;
            this.quality = quality;
        }
    }
    
    static Map<String,EtextInfo> mwToOPEtextAccess = null;
    static String OPEtextAccessQuery = "select distinct ?mw ?acc ?ci ?soft {\n"
            + "    ?eiadm <"+Models.ADM+"syncAgent> <"+Models.BDR+"SAOPT> ;\n"
            + "               <"+Models.ADM+"status> <"+Models.BDA+"StatusReleased> ;\n"
            + "               <"+Models.ADM+"access> ?acc ;\n"
            + "               <"+Models.ADM+"adminAbout> ?ei .\n"
            + "        ?ei <"+Models.BDO+"instanceReproductionOf> ?mw .\n"
            + "  FILTER(?acc = <"+Models.BDA+"AccessOpen> || ?acc = <"+Models.BDA+"AccessFairUse>)\n"
            + "  FILTER(not exists {?mw a <"+Models.BDO+"ImageInstance> })\n"
            + "  OPTIONAL { ?ei <"+Models.BDO+"OPFOCRWordMedianConfidenceIndex> ?ci . }\n"
            + "  OPTIONAL { ?ei <"+Models.BDO+"OPFOCRSoftware> ?soft . }\n"
            + "}";

    static final void add_to_opaccess_cache_fun(final QuerySolution qs, final Map<String,EtextInfo> cache) {
        final String mwlname = qs.getResource("?mw").getLocalName();
        final String acclname = qs.getResource("?acc").getLocalName();
        final String soft = qs.contains("soft") ? qs.getLiteral("soft").getLexicalForm() : null;
        final Float ci = qs.contains("ci") ? qs.getLiteral("ci").getFloat() : null;
        final int etextAccess = "AccessOpen".equals(acclname) ? 3 : 2;
        Float q = ci;
        if ("norbuketaka".equals(soft))
            q = (float) 2.0;
        if (!cache.containsKey(mwlname)) {
            cache.put(mwlname, new EtextInfo(etextAccess, q));
            return;
        }
        final EtextInfo oldei = cache.get(mwlname);
        Float newq = q;
        if (oldei.quality != null) {
            if (q == null)
                newq = oldei.quality;
            else
                newq = Math.max(q, oldei.quality);
        }
        final EtextInfo newei = new EtextInfo(Math.max(etextAccess, oldei.access), newq);
        cache.put(mwlname, newei);
    }
    
    static Map<String,EtextInfo> getMwToOPCache() {
        if (mwToOPEtextAccess != null)
            return mwToOPEtextAccess;
        FusekiHelpers.openConnection(0);
        final RDFConnection conn = FusekiHelpers.fuConn;
        mwToOPEtextAccess = new HashMap<>();
        final Consumer<QuerySolution> add_to_cache = x -> add_to_opaccess_cache_fun(x, mwToOPEtextAccess);
        try {
            conn.querySelect(OPEtextAccessQuery, add_to_cache);
        } catch (Exception ex) {
            logger.error("cannot run "+OPEtextAccessQuery, ex);
            return null;
        }
        return mwToOPEtextAccess;
    }
    
    static Map<String,List<String>> creatorsTraditionCache = null;
    
    static final void add_to_trad_cache_fun(final QuerySolution qs, final Map<String,List<String>> cache) {
        final String plname = qs.getResource("?p").getLocalName();
        final String t = qs.getResource("?t").getLocalName();
        if (!cache.containsKey(plname))
            cache.put(plname, new ArrayList<>());
        cache.get(plname).add(t);
    }
    
    static Map<String,List<String>> getCreatorsTraditionCache() {
        if (creatorsTraditionCache != null)
            return creatorsTraditionCache;
        final String queryStr = "select ?p ?t where { ?p <"+Models.BDO+"associatedTradition> ?t . FILTER(exists{ ?wac <"+Models.BDO+"agent> ?p . })  }";
        FusekiHelpers.openConnection(0);
        final RDFConnection conn = FusekiHelpers.fuConn;
        creatorsTraditionCache = new HashMap<>();
        final Consumer<QuerySolution> add_to_cache = x -> add_to_trad_cache_fun(x, creatorsTraditionCache);
        try {
            conn.querySelect(queryStr, add_to_cache);
        } catch (Exception ex) {
            logger.error("cannot run "+queryStr, ex);
            return null;
        }
        return creatorsTraditionCache;
    }
    
    static Map<String,List<Integer>> personsCenturyCache = null;
    
    static final void add_to_century_cache_fun(final QuerySolution qs, final Map<String,List<Integer>> cache) {
        final String plname = qs.getResource("?p").getLocalName();
        final Integer t = qs.getLiteral("?c").getInt();
        if (!cache.containsKey(plname))
            cache.put(plname, new ArrayList<>());
        cache.get(plname).add(t);
    }
    
    static Map<String,List<Integer>> getPersonsCenturyCache() {
        if (personsCenturyCache != null)
            return personsCenturyCache;
        final String queryStr = "select ?p ?c where { ?p <"+TMP+"associatedCentury> ?c }";
        FusekiHelpers.openConnection(0);
        final RDFConnection conn = FusekiHelpers.fuConn;
        personsCenturyCache = new HashMap<>();
        final Consumer<QuerySolution> add_to_cache = x -> add_to_century_cache_fun(x, personsCenturyCache);
        try {
            conn.querySelect(queryStr, add_to_cache);
        } catch (Exception ex) {
            logger.error("cannot run "+queryStr, ex);
            return null;
        }
        return personsCenturyCache;
    }
    
    static List<String[]> creator_res_to_labels(final Resource res) {
        final Map<String,List<String[]>> cache = getCreatorsLabelCache();
        return cache.get(res.getLocalName());
    }
    
    static List<String> get_ancestors(final Resource res) {
        // get ancestors of resource, read from Fuseki
        // TODO?
        return null;
    }
    
    static Map<String,List<String[]>> resLabelCache = new HashMap<>();
    static List<String[]> res_to_prefLabels(final Resource res) {
        final String resLname = res.getLocalName();
        if (resLabelCache.containsKey(resLname))
            return resLabelCache.get(resLname);
        final Model m = res_to_model(res);
        if (m == null) {
            logger.error("could not find model for "+res.getLocalName());
            resLabelCache.put(resLname, null);
            return null;
        }
        if (!m.contains(null, status, statusReleased)) {
            resLabelCache.put(resLname, null);
            return null;
        }
        final List<String[]> litlist = new ArrayList<>();
        final StmtIterator si = m.listStatements(res, SKOS.prefLabel, (RDFNode) null);
        while (si.hasNext()) {
            final Literal l = si.next().getLiteral();
            final String[] norm = normalize_lit(l);
            litlist.add(norm);
        }
        resLabelCache.put(resLname, litlist);
        return litlist;
    }
    
    
    static void add_ext_prefLabel(final String key, Resource res, ObjectNode doc) {
        final List<String[]> norms = res_to_prefLabels(res);
        if (norms == null)
            return;
        for (final String[] norm : norms)
            add_normalized_to_key(key, norm, doc);
    }
    
    static void add_complete(final Boolean complete, final ObjectNode doc) {
        // don't replace when true:
        if (doc.has("complete") && doc.get("complete").asBoolean())
            return;
        doc.put("complete", complete);
    }
    
    static String[] normalize_lit(final Literal l) {
        // TODO: ewts to Unicode to ewts?
        String lexr = l.getLexicalForm();
        boolean foundLanguage = false;
        String lt = l.getLanguage().toLowerCase().replace('-', '_').replace("hant", "hani").replace("hans", "hani");
        if (lt.equals("zh") || lt.endsWith("hani")) {
            lt = "hani";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.endsWith("twktt")) {
            lt = "iast";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.endsWith("iast")) {
            lt = "iast";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.equals("my") || lt.endsWith("mymr")) {
            lt = "mymr";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.equals("km") || lt.endsWith("khmr")) {
            lt = "khmr";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.endsWith("ewts")) {
            lt = "bo_x_ewts";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.equals("sa_alalc97")) {
            lt = "iast";
            foundLanguage = true;
        }
        if (!foundLanguage && (lt.equals("bo") || lt.equals("dz") || lt.endsWith("_tibt"))) {
            lexr = ewtsc.toWylie(lexr);
            lt = "bo_x_ewts";
            foundLanguage = true;
        }
        if (!foundLanguage && lt.equals("bo_alalc97")) {
            lexr = TransConverter.alalcToEwts(lexr);
            lt = "bo_x_ewts";
            foundLanguage = true;
        }
        if (!foundLanguage && (lt.equals("en_x_mixed") || lt.equals("bo_x_mixed") || lt.equals("bo_x_phon_en_m_tbrc") || lt.equals("bo_x-phon_en_m_thlib") || lt.equals("bo_x_phon_en"))) {
            lt = "en";
            foundLanguage = true;
        }
        if (!foundLanguage && !lt.isEmpty())
            lt = "en";
        return new String[] {lexr, lt};
    }
    
    static final Resource SerialInstance = ResourceFactory.createResource(Models.BDO+"SerialInstance");
    static final Resource Instance = ResourceFactory.createResource(Models.BDO+"Instance");
    
    static void addExtent(final ObjectNode doc, final Model m, final Resource mainRes) {
        // first try extent statement:
        final Statement emst = mainRes.getProperty(ResourceFactory.createProperty(Models.BDO, "extentStatement"));
        if (emst != null) {
            final String emstr = emst.getString();
            doc.put("extent", emstr);
            return;
        }
        // then numberofvolumes:
        final Statement nbvst = mainRes.getProperty(ResourceFactory.createProperty(Models.BDO, "numberOfVolumes"));
        if (nbvst != null) {
            final Integer nbv = nbvst.getInt();
            doc.put("extent", nbv+" v.");
            return;
        }
        // then contentLocation:
        final Resource cl = mainRes.getPropertyResourceValue(ResourceFactory.createProperty(Models.BDO, "ContentLocation"));
        if (cl != null) {
            // first look at the number of volumes:
            final Statement clvst = cl.getProperty(ResourceFactory.createProperty(Models.BDO, "contentLocationVolume"));
            final Statement clevst = cl.getProperty(ResourceFactory.createProperty(Models.BDO, "contentLocationEndVolume"));
            if (clvst != null && clevst != null) {
                final int clv = clvst.getInt();
                final int clev = clevst.getInt();
                final int nbv = clev - clv + 1;
                if (nbv > 1) {
                    doc.put("extent", nbv+" v.");
                    return;
                }
            }
            // then number of pages
            final Statement clpst = cl.getProperty(ResourceFactory.createProperty(Models.BDO, "contentLocationPage"));
            final Statement clepst = cl.getProperty(ResourceFactory.createProperty(Models.BDO, "contentLocationEndPage"));
            if (clpst != null && clepst != null) {
                final int clp = clpst.getInt();
                final int clep = clepst.getInt();
                final int nbp = clep - clp + 1;
                doc.put("extent", nbp+" p.");
                return;
            }
        }
    }
    
    static void addModelToESDoc(final Model m, final ObjectNode doc, final String main_lname, boolean add_admin, boolean add_type, boolean add_prefLabel) {
        final Resource mainRes = m.createResource(Models.BDR+main_lname);
        final StmtIterator si = m.listStatements(mainRes, null, (RDFNode) null);
        if (main_lname.startsWith("MW")) {
            doc.put("scans_access", 1);
            doc.put("join_field", "instance");
            final Map<String,EtextInfo> MWToOPAccess = getMwToOPCache();
            if (MWToOPAccess.containsKey(main_lname)) {
                doc.put("etext_access", MWToOPAccess.get(main_lname).access);
                doc.put("etext_quality", MWToOPAccess.get(main_lname).quality);
            } else {
                doc.put("etext_access", 1);
            }
            addExtent(doc, m, mainRes);
        }
        if (main_lname.startsWith("IE")) {
            // for etexts currently on the git repos, we have two cases: IE4CZ5369 and IE23703 are paginated, the rest is not:
            if ("IE4CZ5369".equals(main_lname) || "IE23703".equals(main_lname)) {
                doc.put("etext_quality", (float) 4.0);
            } else {
                doc.put("etext_quality", (float) 3.0);
            }
        }
        if (main_lname.startsWith("P")) {
            // add century for persons (not in the ttl data but from a cache)
            final Map<String,List<Integer>> agentToCent = getPersonsCenturyCache();
            if (agentToCent.containsKey(main_lname))
                add_centuries(doc, agentToCent.get(main_lname));
        }
        while (si.hasNext()) {
            final Statement s = si.next();
            PropInfo pinfo = propInfoMap.get(s.getPredicate());
            if (pinfo == null) {
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "instanceEvent")))
                    add_event(s.getResource(), doc);
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "isComplete")))
                    add_complete(s.getBoolean(), doc);
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "personEvent")))
                    add_event(s.getResource(), doc);
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "placeLat")))
                    add_gis(mainRes, doc);
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "qualityGrade")))
                    add_quality(mainRes, s.getInt(), doc);
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "creator")))
                    add_creator(s.getResource(), doc);
                continue;
            }
            if (s.getPredicate().equals(RDF.type) && !add_type)
                continue;
            // hack for a bug in the data, some instances only have the SerialInstance type
            if ("type".equals(pinfo.key_base) && s.getResource().equals(SerialInstance)) {
                add_associated(Instance, pinfo.key_base, doc, false);
                continue;
            }
            switch (pinfo.pt) {
            case PT_DIRECT:
                if (!add_prefLabel && "prefLabel".equals(pinfo.key_base))
                    pinfo = new PropInfo(PT_DIRECT, "altLabel", null);
                add_direct(pinfo, s.getLiteral(), doc);
                break;
            case PT_NESTED:
                add_nested(s.getResource(), pinfo.key_base, doc, false);
                break;
            case PT_LABEL_EXT:
                add_associated(s.getResource(), pinfo.key_base+"_res", doc, pinfo.addToAssociated);
                add_ext_prefLabel(pinfo.key_base, s.getResource(), doc);
                break;
            case PT_SPECIAL:
                if (s.getObject().isLiteral()) {
                    logger.error("needed resource but found literal");
                    continue;
                }
                add_special(pinfo, s.getResource(), doc);
                break;
            case PT_RES_ONLY:
                add_associated(s.getResource(), pinfo.key_base, doc, pinfo.addToAssociated);
                break;
            case PT_MERGE:
                add_merged(s.getResource(), doc, pinfo.addToAssociated);
                break;
            case PT_IGNORE:
            default:
                continue;
            }
        }
        add_scores(main_lname, doc);
        if (add_admin) {
            add_admin_to_doc(m.createResource(Models.BDA+main_lname), doc);
        }
        post_process_labels(doc);
    }
    
    private static void add_quality(Resource mainRes, int qualitygrade, ObjectNode doc) {
        // if not scans not sure what to do
        if (!mainRes.getLocalName().startsWith("W"))
            return;
        // we assume that quality grade is between 0 and 5
        float grade = (qualitygrade + 1) / (float) 6;
        if (!doc.has("scans_quality")) {
            doc.put("scans_quality", grade);
            return;
        }
        doc.put("scans_quality", Math.max(doc.get("scans_quality").asDouble(), grade));
    }

    static void add_nested(final Resource r, final String key, final ObjectNode doc, final boolean from_creators_cache) {
        // first check if r is already in the property so we don't add it twice:
        final String rlocal = r.getLocalName();
        ArrayNode vals = null;
        if (doc.has(key) && doc.get(key).isArray()) {
            vals = (ArrayNode) doc.get(key);
            for (JsonNode element : vals) {
                if (element.has("id") && element.get("id").asText().equals(rlocal)) {
                    return;
                }
            }
        } else {
            vals = doc.arrayNode();
            doc.set(key, vals);
        }
        ObjectNode val = doc.objectNode();
        val.put("id", rlocal);
        if (from_creators_cache) {
            final List<String[]> agentLabels = creator_res_to_labels(r);
            if (agentLabels == null) return;
            for (final String[] norm : agentLabels)
                add_normalized_to_key("prefLabel", norm, val);
        }
        vals.add(val);
        add_ext_prefLabel("prefLabel", r, val);
    }

    private static void add_gis(final Resource resource, final ObjectNode doc) {
        final Statement latS = resource.getProperty(resource.getModel().createProperty(Models.BDO, "placeLat"));
        final Statement longS = resource.getProperty(resource.getModel().createProperty(Models.BDO, "placeLong"));
        if (latS == null || longS == null)
            return;
        doc.put("gis_coord", latS.getLiteral().getLexicalForm() + "," + longS.getLiteral().getLexicalForm());
    }

    static void add_admin_to_doc(final Resource adminData, ObjectNode root) {
        // add "graphs" array, and earliest creation date
        final Model m = adminData.getModel();
        final Resource graph_r = adminData.getPropertyResourceValue(m.createProperty(Models.ADM, "graphId"));
        if (!root.has("graphs"))
            root.set("graphs", root.arrayNode());
        if (graph_r != null) {;
            ((ArrayNode) root.get("graphs")).add(graph_r.getLocalName());
        } else {
            logger.error("could not find graph id in " + adminData.getLocalName());
            ((ArrayNode) root.get("graphs")).add(adminData.getLocalName());
        }
        final StmtIterator it = m.listStatements(null, RDF.type, m.createResource(Models.ADM+"InitialDataCreation"));
        while (it.hasNext()) {
            final Resource le = it.next().getSubject();
            final Statement le_when = le.getProperty(m.createProperty(Models.ADM, "logDate"));
            if (le_when != null) {
                final String le_when_s = le_when.getLiteral().getLexicalForm();
                if (!root.has("creation_date")) {
                    root.put("creation_date", le_when_s);
                } else {
                    final String previous_le_when = root.get("creation_date").asText();
                    if (previous_le_when.compareTo(le_when_s) > 0)
                        root.put("creation_date", le_when_s);
                }
                break;
            }
        }
    }
    
    static boolean has_value_in_key(final ObjectNode doc, final String key, final String value) {
        ArrayNode arrayNode = (ArrayNode) doc.get(key);
        // Check if the value is not already in the ArrayNode
        for (JsonNode node : arrayNode) {
            if (node.asText().equals(value))
                return true;
        }
        return false;
    }
    
    static boolean arraynode_has_value(final ArrayNode arrayNode, final Integer value) {
        // Check if the value is not already in the ArrayNode
        for (JsonNode node : arrayNode) {
            if (node.asInt() == value)
                return true;
        }
        return false;
    }
    
    static void add_event(final Resource event, final ObjectNode doc) {
        final Resource evt_type = event.getPropertyResourceValue(RDF.type);
        final Statement whenSt = event.getProperty(ResourceFactory.createProperty(Models.BDO, "eventWhen"));
        if (whenSt == null)
            return;
        switch (evt_type.getLocalName()) {
        case "PublishedEvent":
            doc.put("publicationDate", whenSt.getString());
            break;
        case "PersonBirth":
            doc.put("birthDate", whenSt.getString());
            break;
        case "PersonDeath":
            doc.put("deathDate", whenSt.getString());
            break;
        case "PersonFlourished":
            doc.put("flourishedDate", whenSt.getString());
            break;
        default:
            break;
        }
    }
    
    static void remove_dups(final ArrayNode prefLabels, final ArrayNode altLabels) {
        final Set<String> set1 = new HashSet<>();
        prefLabels.forEach(item -> set1.add(item.asText()));
        Iterator<JsonNode> iterator = altLabels.iterator();
        while (iterator.hasNext()) {
            JsonNode item = iterator.next();
            if (set1.contains(item.asText())) {
                iterator.remove();
            }
        }
    }

    static void post_process_labels(final ObjectNode doc) {
        // we remove the prefLabels that are in the altLabels
        final Iterator<Map.Entry<String, JsonNode>> iter = doc.fields();
        while (iter.hasNext()) {
            final Map.Entry<String, JsonNode> e = iter.next();
            final String key = e.getKey();
            if (key.startsWith("altLabel")) {
                final String prefKey = "pref" + key.substring(3);
                if (doc.has(prefKey)) {
                    remove_dups((ArrayNode) doc.get(prefKey), (ArrayNode) e.getValue());
                    if (e.getValue().isEmpty()) {
                        iter.remove();
                    }
                }
            }
        }
    }
    
    static void add_lit_to_key(String key_base, final Literal l, final ObjectNode doc) {
        String[] normalized = normalize_lit(l);
        add_normalized_to_key(key_base, normalized, doc);
    }

    static void add_normalized_to_key(String key_base, String[] normalized, final ObjectNode doc) {
        if (normalized == null || normalized[0] == null || normalized[0].isEmpty())
            return;
        if (!normalized[1].isEmpty())
            key_base += "_" + normalized[1];
        if (!doc.hasNonNull(key_base))
            doc.set(key_base, doc.arrayNode());
        ArrayNode arrayNode = (ArrayNode) doc.get(key_base);
        // Check if the value is not already in the ArrayNode
        boolean exists = has_value_in_key(doc, key_base, normalized[0]);
        if (exists)
            return;
        if (key_base.startsWith("altLabel")) {
            final String prefLabelKey = "prefLabel_"+normalized[1];
            if (doc.hasNonNull(prefLabelKey))
                exists = has_value_in_key(doc, prefLabelKey, normalized[0]);
        }
        // If the value is not present, add it
        if (!exists)
            arrayNode.add(normalized[0]);
    }
    
    static void add_associated(final Resource r, final String prop, final ObjectNode doc, final boolean add_to_associated) {
        add_associated(r.getLocalName(), prop, doc, add_to_associated);
    }
    
    // not all collections should be in the "inCollection" field (that is used for aggregation)
    // but all collections should be in the associated_res field
    // so that we don't have Wisdom masters, etc. in the aggregates but we still can find them
    static final List<String> collectionsiInFacets = Arrays.asList("PR1PL480", "PR1LOKESH01", "PR1NEPAL00", "PR1KDPP00", "PR1NLM00", "PR1FPL01", "PR1NGMPP00", "PR1TIBET00", "PR1CIHTS00", "PR1VIENNA00", "PR1NCLK");
    static boolean collectionInFacets(final String collection) {
        return collectionsiInFacets.contains(collection);
    }

    static void add_associated(final String r, final String prop, final ObjectNode doc, final boolean add_to_associated) {
        if (!"inCollection".equals(prop) || collectionInFacets(r)) {
            if (!doc.has(prop))
                doc.set(prop, doc.arrayNode());
            if (!has_value_in_key(doc, prop, r))
                ((ArrayNode) doc.get(prop)).add(r);
        }
        if (add_to_associated)
            add_associated(r, "associated_res", doc, false);
    }
    
    public final static Map<String, DocType> prefixToDocType = new HashMap<>();
    static {
        prefixToDocType.put("WAS", DocType.WORK);
        prefixToDocType.put("ITW", DocType.ITEM);
        prefixToDocType.put("WA", DocType.WORK);
        prefixToDocType.put("MW", DocType.INSTANCE);
        prefixToDocType.put("PR", DocType.COLLECTION);
        prefixToDocType.put("IE", DocType.EINSTANCE);
        prefixToDocType.put("IT", DocType.ITEM);
        prefixToDocType.put("W", DocType.IINSTANCE);
        prefixToDocType.put("P", DocType.PERSON);
        prefixToDocType.put("G", DocType.PLACE);
        prefixToDocType.put("R", DocType.OFFICE);
        prefixToDocType.put("L", DocType.LINEAGE);
        prefixToDocType.put("C", DocType.CORPORATION);
        prefixToDocType.put("T", DocType.TOPIC);
        prefixToDocType.put("O", DocType.OUTLINE);
    }
    
    static String lname_to_fpath(final String lname, DocType type) {
        final String localPath = Models.getMd5(lname)+"/"+lname+".trig";
        final String prefix = getTypePrefix(lname);
        if (prefix == null) return null;
        if (type == null)
            type = prefixToDocType.get(prefix);
        if (type == null) return null;
        return GitToDB.gitDir + type + 's' + GitHelpers.localSuffix + "/" + localPath;
    }
    
    static Model res_to_model(final Resource r) {
        final String localPath = Models.getMd5(r.getLocalName())+"/"+r.getLocalName()+".trig";
        final String prefix = getTypePrefix(r.getLocalName());
        if (prefix == null) return null;
        final DocType type = prefixToDocType.get(prefix);
        if (type == null) return null;
        final String fPath = GitToDB.gitDir + type + 's' + GitHelpers.localSuffix + "/" + localPath;
        final Dataset ds = TransferHelpers.datasetFromPath(fPath, type, r.getLocalName());
        if (ds == null) return null;
        return ds.getNamedModel(Models.BDG+r.getLocalName());
    }
    
    // rank_features must be strictly positive (non-zero)
    // etext access:
    //   1: no access
    //   2: search only
    //   3: open access

    // scans access:
    //   1: no access
    //   2: extract only
    //   3: IA
    //   4: open access through IIIF
    //   5: open access on BDRC
    
    final static Resource imageInstance = ResourceFactory.createResource(Models.BDO+"ImageInstance");
    final static Property inCollection = ResourceFactory.createProperty(Models.BDO+"inCollection");
    final static Resource PR0ET009 = ResourceFactory.createResource(Models.BDR+"PR0ET009");
    final static Resource etextInstance = ResourceFactory.createResource(Models.BDO+"EtextInstance");
    final static Property digitalLendingPossible = ResourceFactory.createProperty(Models.BDO+"digitalLendingPossible");
    final static Property restrictedInChina = ResourceFactory.createProperty(Models.ADM+"restrictedInChina");
    final static Property access = ResourceFactory.createProperty(Models.ADM+"access");
    final static Resource accessOpen = ResourceFactory.createProperty(Models.BDA+"AccessOpen");
    final static Resource accessFairUse = ResourceFactory.createProperty(Models.BDA+"AccessFairUse");
    final static Property volumePagesTotal = ResourceFactory.createProperty(Models.BDO+"volumePagesTotal");
    final static Property hasIIIFManifest = ResourceFactory.createProperty(Models.BDO+"hasIIIFManifest");
    final static Resource Synced = ResourceFactory.createResource(Models.ADM+"Synced");
    final static Resource LGIGS001 = ResourceFactory.createResource(Models.BDA+"LGIGS001");
    final static Resource LGIGS002 = ResourceFactory.createResource(Models.BDA+"LGIGS002");
    final static Resource LGIGS003 = ResourceFactory.createResource(Models.BDA+"LGIGS003");
    final static Property logDate = ResourceFactory.createProperty(Models.ADM+"logDate");
    final static Property logEntry = ResourceFactory.createProperty(Models.ADM+"logEntry");
    
    static String get_first_sync_date(final Model m) {
        String firstSyncDate = null;
        if (m.contains(null, logEntry, LGIGS001) || m.contains(null, logEntry, LGIGS002) || m.contains(null, logEntry, LGIGS003))
            return "2016-03-30T16:20:30.571Z";
        final StmtIterator si = m.listStatements(null, RDF.type, Synced);
        while (si.hasNext()) {
            final Resource le = si.next().getSubject();
            final Statement n = le.getProperty(logDate);
            if (n != null) {
                final String dateStr = n.getLiteral().getLexicalForm();
                if (firstSyncDate == null)
                    firstSyncDate = dateStr;
                else
                    firstSyncDate = firstSyncDate.compareTo(dateStr) < 0 ? firstSyncDate : dateStr;
            }
        }
        return firstSyncDate;
    }
    
    static final LocalDate startDate = LocalDate.of(2016, 1, 1);
    static final LocalDate endDate = LocalDate.of(2026, 5, 1);
    static final long totalDays = ChronoUnit.DAYS.between(startDate, endDate);
    final static void add_first_sync_date(final Model m, final ObjectNode doc, final String key) {
        final String modelFirstSyncDate = get_first_sync_date(m);
        if (modelFirstSyncDate == null)
            return;
        LocalDate syncDate;
        try {
            syncDate = LocalDate.parse(modelFirstSyncDate, DateTimeFormatter.ISO_DATE_TIME);
        } catch (DateTimeParseException e) {
            logger.error("cannot parse "+modelFirstSyncDate);
            return;
        }
        long daysFromStart = ChronoUnit.DAYS.between(startDate, syncDate);
        float freshness = (float) daysFromStart / totalDays;
        if (!doc.has(key)) {
            doc.put(key, modelFirstSyncDate);
            doc.put("scans_freshness", freshness);
            return;
        }
        final String docFirstSyncDate = doc.get(key).asText();
        if (docFirstSyncDate.compareTo(modelFirstSyncDate) < 0)
            return;
        doc.put("scans_freshness", freshness);
        doc.put(key, modelFirstSyncDate);
    }

    static void add_access(final Model m, final ObjectNode doc) {
        if (m.contains(null, restrictedInChina, m.createTypedLiteral(true)))
            doc.put("ric", true);
        if (m.contains(null, RDF.type, imageInstance)) {
            add_first_sync_date(m, doc, "firstScanSyncDate");
            // check if there is at least one volume with images
            final NodeIterator ni = m.listObjectsOfProperty(volumePagesTotal);
            boolean hasVolumeWithImages = false;
            while (ni.hasNext()) {
                final int n = ni.next().asLiteral().getInt();
                if (n > 2) {
                    hasVolumeWithImages = true;
                    break;
                }
            }
            boolean hasIiif = false;
            if (!hasVolumeWithImages) {
                hasVolumeWithImages = m.contains(null, hasIIIFManifest, (RDFNode) null);
                hasIiif = true;
            }
            if (!hasVolumeWithImages)
                return;
            JsonNode current_accessN = doc.get("scans_access");
            int current_access = 1;
            if (current_accessN != null)
                current_access = current_accessN.asInt();
            int new_access = 1;
            if (m.contains(null, access, accessOpen)) {
                new_access = hasIiif ? 4 : 5;
            } else if (m.contains(null, access, accessFairUse)) {
                if (m.contains(null, digitalLendingPossible, m.createTypedLiteral(false)))
                    new_access = 2;
                else
                    new_access = 3;
            } else {
                new_access = 1;
            }
            if (new_access > current_access) {
                doc.put("scans_access", new_access);
            }
        } else if (m.contains(null, RDF.type, etextInstance)) {
            // TODO: export access of openpecha
            JsonNode current_accessN = doc.get("etext_access");
            int current_access = 1;
            if (current_accessN != null)
                current_access = current_accessN.asInt();
            int new_access = 1;
            if (m.contains(null, access, accessOpen)) {
                new_access = 3;
            } else if (m.contains(null, access, accessFairUse)) {
                new_access = 2;
            } else {
                new_access = 1;
            }
            if (new_access > current_access) {
                doc.put("etext_access", new_access);
            }
        }
    }
    
    static void add_merged(final Resource r, final ObjectNode doc, final boolean addToAssociated) {
        final Model m = res_to_model(r);
        if (m == null) {
            logger.error("could not find model for "+r.getLocalName());
            return;
        }
        // ignore non-released, etexts from Namsel
        if (!m.contains(null, status, statusReleased) || m.contains(null, inCollection, PR0ET009))
            return;
        // TODO: get max for publication date?
        add_access(m, doc);
        add_associated(r, "merged", doc, addToAssociated);
        addModelToESDoc(m, doc, r.getLocalName(), false, false, false);
    }
    
    static void add_direct(final PropInfo pinfo, final Literal l, final ObjectNode doc) {
        add_lit_to_key(pinfo.key_base, l, doc);
    }
    
    static void add_from_ont_label(final PropInfo pinfo, final Resource r, final ObjectNode doc) {
        add_associated(r, pinfo.key_base, doc, false);
        final Model ont = TransferHelpers.ontModel;
        Property labelP = SKOS.prefLabel;
        if (ont.contains(r, RDFS.label))
            labelP = RDFS.label;
        final StmtIterator si = ont.listStatements(r, labelP, (RDFNode) null);
        while (si.hasNext()) {
            add_lit_to_key(pinfo.key_base, si.next().getLiteral(), doc);
        }
    }
    
    static void add_special(final PropInfo pinfo, final Resource obj, final ObjectNode doc) {
        final StmtIterator si = obj.getModel().listStatements(obj, pinfo.subProp, (RDFNode) null);
        while (si.hasNext()) {
            add_lit_to_key(pinfo.key_base, si.next().getLiteral(), doc);
        }
    }
    
    public static final Map<String,String> roleLnameTokey = new HashMap<>();
    static {
        roleLnameTokey.put("R0ER0011", "author");
        roleLnameTokey.put("R0ER0014", "author");
        roleLnameTokey.put("R0ER0016", "author");
        roleLnameTokey.put("R0ER0017", "translator");
        roleLnameTokey.put("R0ER0018", "translator");
        roleLnameTokey.put("R0ER0019", "author");
        roleLnameTokey.put("R0ER0020", "author");
        roleLnameTokey.put("R0ER0023", "translator");
        roleLnameTokey.put("R0ER0025", "author"); // terton?
        roleLnameTokey.put("R0ER0026", "translator");
        roleLnameTokey.put("R0ER0027", "author");
    }
    
    static void add_centuries(final ObjectNode doc, final List<Integer> cents) {
        if (!doc.has("associatedCentury"))
            doc.set("associatedCentury", doc.arrayNode());
        final ArrayNode cs = (ArrayNode) doc.get("associatedCentury");
        for (final Integer c : cents) {
            if (!arraynode_has_value(cs, c))
                cs.add(c);
        }
    }
    
    static void add_creator(final Resource creatorNode, final ObjectNode doc) {
        final Resource role = creatorNode.getPropertyResourceValue(ResourceFactory.createProperty(Models.BDO, "role"));
        final String key_base = roleLnameTokey.get(role.getLocalName());
        if (key_base == null) return;
        final Resource agent = creatorNode.getPropertyResourceValue(ResourceFactory.createProperty(Models.BDO, "agent"));
        if (agent == null) return;
        add_associated(agent, key_base, doc, true);
        final Map<String,List<String>> agentToTrad = getCreatorsTraditionCache();
        final String agentLocal = agent.getLocalName();
        if (agentToTrad.containsKey(agentLocal)) {
            for (final String t : agentToTrad.get(agentLocal)) {
                add_associated(t, "associatedTradition", doc, false);
            }
        }
        if ("author".equals(key_base)) {
            final Map<String,List<Integer>> agentToCent = getPersonsCenturyCache();
            if (agentToCent.containsKey(agentLocal))
                add_centuries(doc, agentToCent.get(agentLocal));
            final List<String[]> agentLabels = creator_res_to_labels(agent);
            if (agentLabels == null) return;
            for (final String[] norm : agentLabels)
                add_normalized_to_key("authorName", norm, doc);
        }
    }
    
    static void save_as_json(final ObjectNode doc, final String filePath) {
        try {
            om.writer().writeValue(new File(filePath), doc);
        } catch (IOException e) {
            logger.error("cannot write %s", filePath, e);
        }
    }
    
    public static void createDirIfNotExists(final String dir) {
        final File theDir = new File(dir);
        if (!theDir.exists()) {
            try {
                theDir.mkdirs();
            }
            catch(SecurityException se) {
                System.err.println("could not create directory, please fasten your seat belt");
            }
        }
    }
    
    static String lname_to_json_path(final String lname, final DocType type) {
        final String hashtext = Models.getMd5(lname);
        final String dir = jsonfolder + type.toString() + "/" + hashtext.toString()+"/";
        return dir + lname + ".json";
    }
    
    public static BulkRequest.Builder br = null;
    public static OpenSearchClient osc = null;
    public static int nb_in_batch = 0;
    
    static ObjectNode getDoc(final String id) {
        if (osc == null)
            try {
                osc = OSClient.create();
            } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e1) {
                logger.error("cannot create client", e1);
                return null;
            }
        try {
            GetResponse<ObjectNode> response = osc.get(g -> {
                g.index(indexName).id(id);
                return g;
             }, ObjectNode.class);
             if (!response.found())
                 return null;
             return response.source();
        } catch (OpenSearchException | IOException e) {
            logger.error("error fetching doc", e);
            return null;
        }
    }
    
    static void upload(final ObjectNode doc, final String main_lname, final DocType type) {
        if (todisk) {
            final String hashtext = Models.getMd5(main_lname);
            final String dir = jsonfolder + type.toString() + "/" + hashtext.toString()+"/";
            createDirIfNotExists(dir);
            final String filePath = dir + main_lname + ".json";
            save_as_json(doc, filePath);
            return;
        }
        if (osc == null)
            try {
                osc = OSClient.create();
            } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e1) {
                logger.error("cannot create client", e1);
                return;
            }
        if (br == null)
            br = new BulkRequest.Builder();
            br.operations(op -> op           
                .index(idx -> idx            
                    .index(indexName)       
                    .id(main_lname)
                    .document(doc)
                )
            );
        nb_in_batch += 1;
        if (nb_in_batch > FusekiHelpers.esBulkSize) {
            logger.info("transfer {} docs", nb_in_batch);
            nb_in_batch = 0;
            BulkResponse result = null;
            try {
                result = osc.bulk(br.build());
            } catch (OpenSearchException | IOException e) {
                logger.error("exception in bulk operation", e);
            }
            if (result.errors()) {
                logger.error("Bulk had errors");
                for (BulkResponseItem item: result.items()) {
                    if (item.error() != null) {
                        logger.error(item.error().reason());
                    }
                }
            }
            br = null;
        }
    }
    
    static String getLastRevision(DocType type) {
        if (todisk) {
            final File file = new File(lname_to_json_path("systemrev", type));
            if (!file.exists())
                return null;
            try {
                JsonNode obj = om.readTree(file);
                return obj.get("last_rev").asText();
            } catch (IOException e) {
                logger.error("cannot read from %s", file, e);
                return null;
            }
        }
        if (osc == null)
            try {
                osc = OSClient.create();
            } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e1) {
                logger.error("cannot create client", e1);
                return null;
            }
        try {
            GetResponse<ObjectNode> response = osc.get(b -> b.index(indexName).id("systemrev-"+type.toString()), ObjectNode.class);
            if (!response.found())
                return null;
            return response.source().get("last_rev").asText();
        } catch (OpenSearchException | IOException e) {
            logger.error("cannot get document", e);
            return null;
        }
    }
    
    static void setLastRevision(String rev, DocType type) {
        ObjectNode root = om.createObjectNode();
        root.put("last_rev", rev);
        upload(root, "systemrev-"+type.toString(), type);
    }
    
    static void remove(String mainId, final DocType type) {
        if (todisk) {
            final File file = new File(lname_to_json_path(mainId, type));
            if (file.exists())
                file.delete();
            return;
        }
        if (osc == null)
            try {
                osc = OSClient.create();
            } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e1) {
                logger.error("cannot creat client", e1);
            }
        try {
            osc.delete(b -> b.index(indexName).id("1"));
        } catch (OpenSearchException | IOException e) {
            logger.error("cannot remove document", e);
        }

    }
    
    static void finishDatasetTransfers() {
        if (br != null) {
            logger.info("transfer {} docs", nb_in_batch);
            BulkResponse result = null;
            try {
                result = osc.bulk(br.build());
            } catch (OpenSearchException | IOException e) {
                logger.error("exception in bulk operation", e);
            }
            if (result.errors()) {
                logger.error("Bulk had errors");
                for (BulkResponseItem item: result.items()) {
                    if (item.error() != null) {
                        logger.error(item.error().reason());
                    }
                }
            }
        }
        br = null;
    }
    
    static void upload_outline(final String olname, final Model m) {
        final Resource or = m.createResource(Models.BDR+olname);
        final Resource mw = or.getPropertyResourceValue(m.createProperty(Models.BDO, "outlineOf"));
        final ObjectNode rootDoc = getDoc(mw.getLocalName());
        // TODO: get access from mw
        upload_outline_children_rec(mw, olname, rootDoc);
    }
    
    public static final List<String> fieldsToCopy = Arrays.asList(new String[] { "language", "script", "hasSourcePrintery", "printMethod", "inCollection", "firstScanSyncDate", "db_score", "db_score_in_type" });
    public static final List<String> fieldsToCopyReplace = Arrays.asList(new String[] {"scans_access", "scans_freshness", "scans_quality", "etext_access", "etext_freshness", "etext_quality"});
    public static final List<String> fieldsToCopyWithPrefix = Arrays.asList(new String[] { "pop_score", "pop_score_in_type" });
    static void copyRootFields(final ObjectNode part, final ObjectNode rootNode) {
        for (final String field : fieldsToCopy) {
            if (part.has(field) || rootNode == null || rootNode.get(field) == null)
                continue;
            part.set(field, rootNode.get(field));
        }
        for (final String field : fieldsToCopyReplace) {
            if (rootNode == null || rootNode.get(field) == null)
                continue;
            part.set(field, rootNode.get(field));
        }
        for (final String field : fieldsToCopyWithPrefix) {
            if (rootNode == null || rootNode.get(field) == null)
                continue;
            part.set("root_"+field, rootNode.get(field));
        }
    }
    
    public static final Property partOf = ResourceFactory.createProperty(Models.BDO, "partOf"); 
    static void upload_outline_children_rec(final Resource parent, final String olname, final ObjectNode rootNode) {
        final ResIterator childiter = parent.getModel().listSubjectsWithProperty(partOf, parent);
        while (childiter.hasNext()) {
            final Resource child = childiter.next();
            ObjectNode childDoc = om.createObjectNode();
            if (!childDoc.has("graphs"))
                childDoc.set("graphs", childDoc.arrayNode());
            ((ArrayNode) childDoc.get("graphs")).add(olname);
            addModelToESDoc(parent.getModel(), childDoc, child.getLocalName(), false, false, true);
            copyRootFields(childDoc, rootNode);
            upload(childDoc, child.getLocalName(), DocType.OUTLINE);
            upload_outline_children_rec(child, olname, rootNode);
        }
    }
    
    public static final boolean shouldIgore(final String lname) {
        if (!lname.startsWith("WA"))
            return false;
        final Map<String,Boolean> worksToInclude = getWorksToSend();
        return !worksToInclude.containsKey(lname);
    }

    public static final Property status = ResourceFactory.createProperty(Models.ADM, "status");
    public static final Resource statusReleased = ResourceFactory.createResource(Models.BDA + "StatusReleased");
    public static void upload(DocType type, String mainId, String filePath, Model model, String graphUri) {
        if (!model.contains(null, status, statusReleased)) {
            logger.info("ignore non-released {}", graphUri);
            return; // TODO: remove?
        }
        //final String rev = GitHelpers.getLastRefOfFile(type, filePath);
        // TODO: add rev?
        if (type == DocType.OUTLINE) {
            upload_outline(mainId, model);
            return;
        }
        ObjectNode root = om.createObjectNode();
        addModelToESDoc(model, root, mainId, true, true, true);
        upload(root, mainId, type);
    }
    
}
