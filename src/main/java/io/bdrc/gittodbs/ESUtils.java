package io.bdrc.gittodbs;

import java.io.File;
import java.io.IOException;
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
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.bdrc.gittodbs.TransferHelpers.DocType;
import io.bdrc.libraries.Models;

public class ESUtils {
    
    public static final Logger logger = LoggerFactory.getLogger(ESUtils.class);
    public static String jsonfolder = "json/";
    
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
    // TODO: special properties like event and creator need their own thing
    
    final static class PropInfo {
        int pt;
        String key_base = null;
        Property subProp = null;
        
        PropInfo(final int pt, final String key_base, final Property subProp) {
            this.pt = pt;
            this.key_base = key_base;
            this.subProp = subProp;
        }
        
    }
    
    private static final Map<Property, PropInfo> propInfoMap = new HashMap<>();
    static {
        // General properties
        propInfoMap.put(SKOS.prefLabel, new PropInfo(PT_DIRECT, "prefLabel", null));
        propInfoMap.put(SKOS.altLabel, new PropInfo(PT_DIRECT, "altLabel", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BF, "inCollection"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(SKOS.definition, new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(RDFS.comment, new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "note"), new PropInfo(PT_SPECIAL, "comment", ResourceFactory.createProperty(Models.BDO, "noteText")));

        // Person properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "associatedTradition"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "personGender"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "personName"), new PropInfo(PT_SPECIAL, "altLabel", RDFS.label));
        
        // MW properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BF, "identifiedBy"), new PropInfo(PT_SPECIAL, "other_id", RDF.value));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "instanceOf"), new PropInfo(PT_MERGE, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "instanceHasReproduction"), new PropInfo(PT_MERGE, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "authorshipStatement"), new PropInfo(PT_DIRECT, "authorshipStatement", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "biblioNote"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "hasSourcePrintery"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "hasTitle"), new PropInfo(PT_SPECIAL, "altLabel", RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "printMethod"), new PropInfo(PT_RES_ONLY, null, RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "script"), new PropInfo(PT_RES_ONLY, null, RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "publisherName"), new PropInfo(PT_DIRECT, "publisherName", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "incipit"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "colophon"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "publisherLocation"), new PropInfo(PT_DIRECT, "publisherLocation", null));
        
        // W / IE properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "scanInfo"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "etextInfo"), new PropInfo(PT_DIRECT, "comment", null));
        
        // WA properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "catalogInfo"), new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "language"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "workIsAbout"), new PropInfo(PT_LABEL_EXT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "workGenre"), new PropInfo(PT_LABEL_EXT, "comment", null));
        
        // MW in outlines properties
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "inRootInstance"), new PropInfo(PT_RES_ONLY, "inRootInstance", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "partOf"), new PropInfo(PT_RES_ONLY, "partOf", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "partType"), new PropInfo(PT_RES_ONLY, "partType", null));
    }
    
    static final ObjectMapper om = new ObjectMapper();
    
    // to get the creator's cache:
    // select ?p ?pl where {
    //   ?p a :Person .
    //   FILTER(exists{ ?wac :agent ?p . })
    //   ?p skos:prefLabel ?pl .
    // }
    
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
            return null;
        }
        return creatorsLabelCache;
    }
    
    static List<String[]> creator_res_to_labels(final Resource res) {
        final Map<String,List<String[]>> cache = getCreatorsLabelCache();
        return cache.get(res.getLocalName());
    }
    
    static List<String> get_ancestors(final Resource res) {
        // get ancestors of resource, read from Fuseki
        return null;
    }
    
    static List<Literal> res_to_prefLabels(final Resource res) {
        // if fromQuery == creator, generate and read from cache
        // TODO
        return null;
    }
    
    static String[] normalize_lit(final Literal l) {
        // TODO: ewts to Unicode to ewts
        // Unicode to ewts
        String lexr = l.getLexicalForm();
        String lt = l.getLanguage().toLowerCase().replace('-', '_').replace("hant", "hani").replace("hans", "hani");
        return new String[] {lexr, lt};
    }
    
    static void addModelToESDoc(final Model m, final ObjectNode doc, final String main_lname, boolean add_admin) {
        final Resource mainRes = m.createResource(Models.BDR+main_lname);
        final StmtIterator si = m.listStatements(mainRes, null, (RDFNode) null);
        while (si.hasNext()) {
            final Statement s = si.next();
            final PropInfo pinfo = propInfoMap.get(s.getPredicate());
            if (pinfo == null) {
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "workGenre")))
                    add_event(s.getResource(), doc);
                if (s.getPredicate().equals(ResourceFactory.createProperty(Models.BDO, "creator")))
                    add_creator(s.getResource(), doc);
                continue;
            }
            switch (pinfo.pt) {
            case PT_DIRECT:
                add_direct(pinfo, s.getLiteral(), doc);
                break;
            case PT_LABEL_ONT:
                add_from_ont_label(pinfo, s.getResource(), doc);
                break;
            case PT_SPECIAL:
                add_special(pinfo, s.getResource(), doc);
                break;
            case PT_RES_ONLY:
                add_associated(s.getResource(), doc);
                break;
            case PT_MERGE:
                add_merged(s.getResource(), doc);
                break;
            case PT_IGNORE:
            default:
                continue;
            }
        }
        if (add_admin)
            add_admin_to_doc(m.createResource(Models.BDA+main_lname), doc);
        post_process_labels(doc);
    }
    
    //static void add_outline_model_to_doc()

    static void add_admin_to_doc(final Resource adminData, ObjectNode root) {
        // add "graphs" array, and earliest creation date
        final Model m = adminData.getModel();
        final String graph_lname = adminData.getPropertyResourceValue(m.createProperty(Models.ADM, "graphId")).getLocalName();
        if (!root.has("graphs"))
            root.set("graphs", root.arrayNode());
        ((ArrayNode) root.get("graphs")).add(graph_lname);
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
        // TODO: add latest_scans_sync_date, latest_etext_sync_date
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
    
    static void add_event(final Resource event, final ObjectNode doc) {
        final Resource evt_type = event.getPropertyResourceValue(RDF.type);
        if (evt_type.getLocalName().equals("PublishedEvent")) {
            final Statement whenSt = event.getProperty(ResourceFactory.createProperty(Models.BDO, "eventWhen"));
            if (whenSt != null) {
                doc.put("publicationDate", whenSt.getString());
            }
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
        // If the value is not present, add it
        if (!exists)
            arrayNode.add(normalized[0]);
    }
    
    static void add_associated(final Resource r, final ObjectNode doc) {
        if (!doc.has("associated_res"))
            doc.set("associated_res", doc.arrayNode());
        ((ArrayNode) doc.get("associated_res")).add(r.getLocalName());
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
    
    static void add_merged(final Resource r, final ObjectNode doc) {
        final Model m = res_to_model(r);
        add_associated(r, doc);
        addModelToESDoc(m, doc, r.getLocalName(), false);
    }
    
    static void add_direct(final PropInfo pinfo, final Literal l, final ObjectNode doc) {
        add_lit_to_key(pinfo.key_base, l, doc);
    }
    
    static void add_from_ont_label(final PropInfo pinfo, final Resource r, final ObjectNode doc) {
        add_associated(r, doc);
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
    
    static void add_ext_labels(final Resource ext, final ObjectNode doc, final String key_base) {
        final List<Literal> prefLabels = res_to_prefLabels(ext);
        for (final Literal l : prefLabels) {
            add_lit_to_key(key_base, l, doc);
        }
    }
    
    static void add_creator(final Resource creatorNode, final ObjectNode doc) {
        final Resource role = creatorNode.getPropertyResourceValue(ResourceFactory.createProperty(Models.BDO, "role"));
        final String key_base = roleLnameTokey.get(role.getLocalName());
        if (key_base == null) return;
        final Resource agent = creatorNode.getPropertyResourceValue(ResourceFactory.createProperty(Models.BDO, "agent"));
        if (agent == null) return;
        add_associated(agent, doc);
        final List<String[]> agentLabels = creator_res_to_labels(agent);
        if (agentLabels == null) return;
        for (final String[] norm : agentLabels)
            add_normalized_to_key(key_base, norm, doc);
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
    
    static void save_file(final ObjectNode doc, final String main_lname, final DocType type) {
        final String hashtext = Models.getMd5(main_lname);
        final String dir = jsonfolder + type.toString() + "/" + hashtext.toString()+"/";
        System.out.println("create if not exists "+dir);
        createDirIfNotExists(dir);
        final String filePath = dir + main_lname + ".json";
        save_as_json(doc, filePath);
    }
    
    static String getLastRevision(DocType type) {
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
    
    static void setLastRevision(String rev, DocType type) {
        ObjectNode root = om.createObjectNode();
        root.put("last_rev", rev);
        save_file(root, "systemrev", type);
    }
    
    static void remove(String mainId, final DocType type) {
        final File file = new File(lname_to_json_path(mainId, type));
        if (file.exists())
            file.delete();
    }
    
    static void finishDatasetTransfers() {
        // TODO?
    }

    public static final Property status = ResourceFactory.createProperty(Models.ADM, "status");
    public static final Resource statusReleased = ResourceFactory.createResource(Models.BDA + "StatusReleased");
    public static void upload(DocType type, String mainId, String filePath, Model model, String graphUri) {
        if (!model.contains(null, status, statusReleased))
            return; // TODO: remove?
        final String rev = GitHelpers.getLastRefOfFile(type, filePath);
        ObjectNode root = om.createObjectNode();
        // TODO: outlines are different
        addModelToESDoc(model, root, mainId, true);
        // TODO: compute an access value for MWs based on their reproductions
        save_file(root, mainId, type);
    }
    
}
