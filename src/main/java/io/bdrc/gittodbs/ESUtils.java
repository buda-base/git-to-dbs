package io.bdrc.gittodbs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.bdrc.libraries.Models;

public class ESUtils {
    
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
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "associatedTradition"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(SKOS.prefLabel, new PropInfo(PT_DIRECT, "prefLabel", null));
        propInfoMap.put(SKOS.altLabel, new PropInfo(PT_DIRECT, "altLabel", null));
        propInfoMap.put(SKOS.definition, new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(RDFS.comment, new PropInfo(PT_DIRECT, "comment", null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "personGender"), new PropInfo(PT_RES_ONLY, null, null));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "personName"), new PropInfo(PT_SPECIAL, "altLabel", RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "hasTitle"), new PropInfo(PT_SPECIAL, "altLabel", RDFS.label));
        propInfoMap.put(ResourceFactory.createProperty(Models.BDO, "note"), new PropInfo(PT_SPECIAL, "comment", ResourceFactory.createProperty(Models.BDO, "noteText")));
        propInfoMap.put(ResourceFactory.createProperty(Models.BF, "identifiedBy"), new PropInfo(PT_SPECIAL, "other_id", RDF.value));
    }
    
    static final ObjectMapper om = new ObjectMapper();
    
    static Map<String,String> res_to_prefLabel_git(final Resource res) {
        // TODO
        return null;
    }
    
    static Map<String,String> res_to_prefLabel_Fuseki(final Resource res) {
        // TODO
        return null;
    }
    
    static Map<String,String> res_to_prefLabel_ontology(final Resource res) {
        // TODO
        return null;
    }
    
    static Map<String,String> res_to_prefLabel(final Resource res) {
        // ontology, if not, if topic, cache, else Fuseki (or git, configurable)
        return null;
    }
    
    static String[] normalize_lit(final Literal l) {
        // TODO: ewts to Unicode to ewts
        // Unicode to ewts
        String lexr = l.getLexicalForm();
        String lt = l.getLanguage().toLowerCase().replace('-', '_').replace("hant", "hani").replace("hans", "hani");
        return new String[] {lexr, lt};
    }

    static Model get_ontology() {
        return null;
    }

    static ObjectNode getESDocument_fromModel(final Model m, final String main_lname, final Resource gitRepo, final String commit) {
        ObjectNode root = om.createObjectNode();
        addModelToESDoc(m, root, main_lname, true);
        return root;
    }

    static ObjectNode getESDocument_fromFile(final String fpath) {
        return null;
    }

    void addESDocument_fromRes(final Resource res, final ObjectNode doc) {
        // merges a document with another
    }
    
    static void addModelToESDoc(final Model m, final ObjectNode doc, final String main_lname, boolean add_admin) {
        final Resource mainRes = m.createResource(Models.BDR+main_lname);
        final StmtIterator si = m.listStatements(mainRes, null, (RDFNode) null);
        while (si.hasNext()) {
            final Statement s = si.next();
            final PropInfo pinfo = propInfoMap.get(s.getPredicate());
            if (pinfo == null) continue;
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
            case PT_IGNORE:
            default:
                continue;
            }
        }
        if (add_admin)
            add_admin_to_doc(m.createResource(Models.BDA+main_lname), doc);
    }

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
        post_process_labels(doc);
    }
    
    static void add_associated(final Resource r, final ObjectNode doc) {
        if (!doc.has("associated_res"))
            doc.set("associated_res", doc.arrayNode());
        ((ArrayNode) doc.get("associated_res")).add(r.getLocalName());
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
    
    // void add_from_ext_label(final Property p, final Resource obj, final ObjectNode doc)
    // void merge_with_ext(final Resource to_merge, final ObjectNode doc)
    // void add_special(final Property p, final Resource obj, final ObjectNode doc)
    // void save_as_json(final Resource res, final ObjectNode doc) 
    // void send_to_es()

    
}
