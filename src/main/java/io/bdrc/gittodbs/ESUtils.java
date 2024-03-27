package io.bdrc.gittodbs;

import java.util.HashMap;
import java.util.Map;

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
    
    private static final Map<Property, Integer> propToPT = new HashMap<>();
    private static final Map<Property, Property> specialToSubprop = new HashMap<>();
    static {
        propToPT.put(ResourceFactory.createProperty(Models.BDO, "associatedTradition"), PT_LABEL_ONT);
        propToPT.put(SKOS.prefLabel, PT_DIRECT);
        propToPT.put(SKOS.altLabel, PT_DIRECT);
        propToPT.put(SKOS.definition, PT_DIRECT);
        propToPT.put(RDFS.comment, PT_DIRECT);
        propToPT.put(ResourceFactory.createProperty(Models.BDO, "personGender"), PT_LABEL_ONT);
        propToPT.put(ResourceFactory.createProperty(Models.BDO, "personName"), PT_SPECIAL);
        propToPT.put(ResourceFactory.createProperty(Models.BDO, "hasTitle"), PT_SPECIAL);
        propToPT.put(ResourceFactory.createProperty(Models.BDO, "note"), PT_SPECIAL);
        propToPT.put(ResourceFactory.createProperty(Models.BF, "identifiedBy"), PT_SPECIAL);
        
        specialToSubprop.put(ResourceFactory.createProperty(Models.BDO, "note"), ResourceFactory.createProperty(Models.BDO, "noteText"));
        specialToSubprop.put(ResourceFactory.createProperty(Models.BDO, "hasTitle"), RDFS.label);
        specialToSubprop.put(ResourceFactory.createProperty(Models.BDO, "personName"), RDFS.label);
        specialToSubprop.put(ResourceFactory.createProperty(Models.BF, "identifiedBy"), RDF.value);
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
        addModelToESDoc(m, root, main_lname);
        return root;
    }

    static ObjectNode getESDocument_fromFile(final String fpath) {
        return null;
    }

    void addESDocument_fromRes(final Resource res, final ObjectNode doc) {
        // merges a document with another
    }
    
    static void addModelToESDoc(final Model m, final ObjectNode doc, final String main_lname) {
        final Resource mainRes = m.createResource(Models.BDR+main_lname);
        final StmtIterator si = m.listStatements(mainRes, null, (RDFNode) null);
        while (si.hasNext()) {
            final Statement s = si.next();
            Integer ptype = propToPT.get(s.getPredicate());
            if (ptype == null) continue;
            switch (ptype) {
            case PT_DIRECT:
                add_direct(s.getPredicate(), s.getLiteral(), doc);
                break;
            case PT_LABEL_ONT:
                add_from_ont_label(s.getPredicate(), s.getResource(), doc);
                break;
            case PT_SPECIAL:
                add_special(s.getPredicate(), s.getResource(), doc);
                break;
            case PT_IGNORE:
            default:
                continue;
            }
        }
    }

    void add_admin(final Resource adminData, ObjectNode root) {
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
    
    static void add_lit_to_key(String key_base, final Literal l, final ObjectNode doc) {
        String[] normalized = normalize_lit(l);
        if (normalized == null || normalized[0] == null || normalized[0].isEmpty())
            return;
        if (!normalized[1].isEmpty())
            key_base += "_" + normalized[1];
        if (!doc.hasNonNull(key_base)) {
            ArrayNode newval = doc.arrayNode();
            newval.add(normalized[0]);
            doc.set(key_base, newval);
            return;
        }
        ArrayNode arrayNode = (ArrayNode) doc.get(key_base);
        // Check if the value is not already in the ArrayNode
        boolean exists = false;
        for (JsonNode node : arrayNode) {
            if (node.asText().equals(normalized[0])) {
                exists = true;
                break;
            }
        }
        // If the value is not present, add it
        if (!exists) {
            arrayNode.add(normalized[0]);
        }
    }
    
    static void add_associated(final Resource r, final ObjectNode doc) {
        if (!doc.has("associated_res"))
            doc.set("associated_res", doc.arrayNode());
        ((ArrayNode) doc.get("associated_res")).add(r.getLocalName());
    }
    
    static void add_direct(final Property p, final Literal l, final ObjectNode doc) {
        String key = p.getLocalName();
        add_lit_to_key(key, l, doc);
    }
    
    static void add_from_ont_label(final Property p, final Resource r, final ObjectNode doc) {
        add_associated(r, doc);
        String key_base = p.getLocalName();
        final Model ont = TransferHelpers.ontModel;
        Property labelP = SKOS.prefLabel;
        if (ont.contains(r, RDFS.label))
            labelP = RDFS.label;
        final StmtIterator si = ont.listStatements(r, labelP, (RDFNode) null);
        while (si.hasNext()) {
            add_lit_to_key(key_base, si.next().getLiteral(), doc);
        }
    }
    
    static void add_special(final Property p, final Resource obj, final ObjectNode doc) {
        String key_base = p.getLocalName();
        final Property subProp = specialToSubprop.get(p);
        final StmtIterator si = obj.getModel().listStatements(obj, subProp, (RDFNode) null);
        while (si.hasNext()) {
            add_lit_to_key(key_base, si.next().getLiteral(), doc);
        }
    }
    
    // void add_from_ext_label(final Property p, final Resource obj, final ObjectNode doc)
    // void merge_with_ext(final Resource to_merge, final ObjectNode doc)
    // void add_special(final Property p, final Resource obj, final ObjectNode doc)
    // void save_as_json(final Resource res, final ObjectNode doc) 
    // void send_to_es()

    
}
