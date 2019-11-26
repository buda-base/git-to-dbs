package io.bdrc.gittodbs;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.eclipse.jgit.treewalk.TreeWalk;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.ewtsconverter.EwtsConverter;
import io.bdrc.gittodbs.TransferHelpers.DocType;

public class LibFormat {
    public static final ObjectMapper om = new ObjectMapper();
    public static final EwtsConverter converter = new EwtsConverter(false, false, false, true);
    public static final Reasoner bdrcReasoner = TransferHelpers.bdrcReasoner;
    
    public static final String PERSON = "person";
    public static final String WORK = "work";
    public static final String WORKPARTS = "workparts";
    
    public static final int maxkeysPerIndex = 20000;

    public static final Map<DocType,Query> typeQuery = new EnumMap<>(DocType.class);
    
    public static Query getQuery(DocType type) {
        if (typeQuery.containsKey(type))
            return typeQuery.get(type);
        // the following nonsense just reads a freaking file
        ClassLoader classLoader = TransferHelpers.class.getClassLoader();
        String filePath = "sparql/" + type + ".sparql";
        if (type == DocType.OUTLINE) {
            filePath = "sparql/" + WORKPARTS + ".sparql";
        }
        System.out.println("open "+filePath);
        InputStream inputStream = classLoader.getResourceAsStream(filePath);
        Scanner s = new Scanner(inputStream);
        s.useDelimiter("\\A");
        String result = s.hasNext() ? s.next() : "";
        s.close();
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        Query res = QueryFactory.create(result.toString());
        typeQuery.put(type, res);
        return res;
    }
    
    public static String toUnicode (final String ewtsString) {
        return converter.toUnicode(ewtsString);
    }
    
    public static String getUnicodeStrFromProp(QuerySolution soln, String var) {
        RDFNode valueN = soln.get(var);
        if (valueN == null)
            return null;
        Literal valueL = valueN.asLiteral();
        if (valueL.getLanguage().endsWith("-x-ewts")) {
            String uniStr = toUnicode(valueL.getString());
            return uniStr;
        }else
            return valueL.getLexicalForm();
    }
    
    public static void addInt(final QuerySolution soln, final Map<String,Object> node, final String prop) {
        if (!soln.contains(prop))
            return;
        final String val = soln.getLiteral(prop).getString();
        try {
            final int valInt = Integer.valueOf(val);
            node.put(prop, valInt);
        } catch (NumberFormatException ex) {
            node.put(prop, val);    
        }
    }

    public static void addStr(final QuerySolution soln, final Map<String,Object> node, final String prop) {
        if (!soln.contains(prop))
            return;
        node.put(prop, soln.getLiteral(prop).getString());
    }

    public static final int BDRlen = TransferHelpers.BDR.length();
    public static String removeBdrPrefix(final String s) {
        return s.substring(BDRlen);
    }
    
    public static void addPrefixedId(final QuerySolution soln, final Map<String,Object> node, final String prop) {
        if (!soln.contains(prop))
            return;
        String fullUri = soln.getResource(prop).getURI();
        node.put(prop, "bdr:"+fullUri.substring(BDRlen));
    }
    
    public static List<String> getTitles(final Resource r, final Model m, Map<String,List<String>> index) {
        List<String> res = new ArrayList<>();
        Statement prefLabelS = r.getProperty(SKOS.prefLabel, "bo-x-ewts");
        if (prefLabelS == null) {
            return null;
        }
        String preflabel = toUnicode(prefLabelS.getString());
        final String idName = "bdr:"+r.getLocalName();
        if (index != null) {
            List<String> idxList = (List<String>) index.computeIfAbsent(preflabel, x -> new ArrayList<String>());
            if (!idxList.contains(idName)) {
                idxList.add(idName);
            }
        }
        res.add(preflabel);
        final StmtIterator titleItr = r.listProperties(m.getProperty(TransferHelpers.BDO, "workTitle"));
        while (titleItr.hasNext()) {
            final Statement t = titleItr.next();
            Statement titleS = t.getObject().asResource().getProperty(RDFS.label, "bo-x-ewts");
            if (titleS != null) {
                String titlelabel = toUnicode(titleS.getString());
                if (!titlelabel.equals(preflabel)) {
                    if (index != null) {
                        List<String> idxList = (List<String>) index.computeIfAbsent(titlelabel, x -> new ArrayList<String>());
                        if (!idxList.contains(idName)) {
                            idxList.add(idName);
                        }
                    }
                    res.add(titlelabel);
                }
                    
            }
        }
        if (res.isEmpty())
            return null;
        return res;
        
        
        
    }
    
    public static List<String> getCreators(final Resource r, final Model m) {
        final StmtIterator eventsItr = r.listProperties(m.getProperty(TransferHelpers.BDO, "creator"));
        List<String> res = new ArrayList<>();
        while (eventsItr.hasNext()) {
            final Statement es = eventsItr.next();
            final Resource aac = es.getObject().asResource();
            final StmtIterator agentItr = aac.listProperties(m.getProperty(TransferHelpers.BDO, "agent"));
            while (agentItr.hasNext()) {
                final Statement as = agentItr.next();
                final Resource c = as.getObject().asResource();
                res.add("bdr:"+c.getLocalName());
            }
        }
        if (res.isEmpty())
            return null;
        return res;
    }
    
    public static void recOutlineChildren(Map<String,Object> curNode, final Resource curNodeRes, final Model m, Map<String,List<String>> index) {
        final StmtIterator partsItr = curNodeRes.listProperties(m.getProperty(TransferHelpers.BDO, "workHasPart"));
        if (partsItr.hasNext()) {
            List<Map<String,Object>> nodes = new ArrayList<>();
            Map<Integer, Map<String,Object>> nodesOrdered = new TreeMap<>();
            curNode.put("nodes", nodes);
            while (partsItr.hasNext()) {
                final Statement s = partsItr.next();
                final Resource part = s.getObject().asResource();
                Map<String,Object> node = new HashMap<>();
                Statement partIdxS = part.getProperty(m.getProperty(TransferHelpers.BDO, "workPartIndex"));
                if (partIdxS == null) {
                    System.out.println("oops!");
                    return;
                }
                int partIdx = partIdxS.getInt();
                node.put("id", "bdr:"+part.getLocalName());
                List<String> titles = getTitles(part, m, index);
                if (titles != null) {
                    node.put("titles", titles);
                }
                List<String> creators = getCreators(part, m);
                if (creators != null) {
                    node.put("creators", creators);
                }
                nodesOrdered.put(partIdx, node);
                recOutlineChildren(node, part, m, index);
            }
            for (Map<String,Object> node : nodesOrdered.values()) {
                nodes.add(node);
            }
        }
    }
    
    public static Map<String, Object> modelToOutline(final String mainId, final Model m, Map<String,List<String>> index) {
        Resource root = m.createResource(TransferHelpers.BDR+mainId);
        if (!root.hasProperty(m.getProperty(TransferHelpers.BDO, "workHasPart"))) {
            System.out.println("notgood");
            return null;
        }
        Map<String,Object> res = new HashMap<>();
        List<String> titles = getTitles(root, m, index);
        if (titles != null) {
            res.put("workTitle", titles);            
        }
        // get nodes!
        recOutlineChildren(res, root, m, index);
        return res;
    }
    
    public static Map<String, Object> modelToJsonObject(final String mainId, final Model m, final DocType type, Map<String,List<String>> index) {
        final Query query = getQuery(type);
        //final InfModel im = TransferHelpers.getInferredModel(m);
        //TransferHelpers.printModel(im);
        final Map<String,Object> res = new HashMap<String,Object>();
        try (QueryExecution qexec = QueryExecutionFactory.create(query, m)) {
            final ResultSet results = qexec.execSelect() ;
            while (results.hasNext()) {
                final QuerySolution soln = results.nextSolution();
                if (!soln.contains("property"))
                    continue;
                String property = soln.get("property").asLiteral().getString();
                System.out.println("property "+property);
                if (property.equals("status")) {
                    if (soln.contains("value") && !soln.get("value").asResource().getLocalName().equals("StatusReleased")) {
                        return null;
                    }
                    if (soln.contains("ric") && soln.get("ric").asLiteral().getBoolean()) {
                        return null;
                    }
                    if (soln.contains("access")) {
                        if (soln.get("access").asResource().getLocalName().startsWith("AccessRestricted")) {
                            return null;
                        }
                        res.put(property, soln.get("access").asResource().getLocalName().substring(6).toLowerCase());
                    }
                    if (soln.contains("status")) {
                        if (soln.get("status").asResource().getLocalName().equals("StatusWithdrawn")) {
                            return null;
                        }
                        res.put(property, soln.get("status").asResource().getLocalName().substring(6).toLowerCase());
                    }
                    continue;
                }
                if (property.equals("volumes[]")) {
                    final Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) res.computeIfAbsent("volumes", x -> new TreeMap<String,Map<String, Object>>());
                    final String nodeId = soln.getLiteral("volNum").getString();
                    final Map<String,Object> node = (Map<String, Object>) nodes.computeIfAbsent(nodeId, x -> new TreeMap<String,Object>());
                    addPrefixedId(soln, node, "v");
                    if (soln.contains("type"))
                        res.put("type", soln.getResource("type").getLocalName());
                    if (soln.contains("forWorkURI")) {
                        addPrefixedId(soln, res, "forWorkURI");
                    }
                    if (soln.contains("etexts")) {
                        String etextsStr = soln.getLiteral("etexts").getString();
                        String[] etextsA = etextsStr.split(";");
                        List<String> etexts = new ArrayList<>();
                        for (String etextStr : etextsA) {
                            etextStr = removeBdrPrefix(etextStr);
                            etexts.add(etextStr);
                        }
                        Collections.sort(etexts);
                        node.put("etexts", etexts);
                    }
                } 
                else if (property.contains("URI")) {
                    final Resource valueURI = soln.get("value").asResource();
                    final String value = "bdr:"+valueURI.getLocalName();
                    if (property.endsWith("[URI]")) {
                        property = property.substring(0, property.length()-5);
                        final List<String> valList = (List<String>) res.computeIfAbsent(property, x -> new ArrayList<String>());
                        valList.add(value);
                    } else {
                        property = property.substring(0, property.length()-3);
                        res.put(property, value);
                    }
                } else {
                    final String value = getUnicodeStrFromProp(soln, "value");
                    if (value == null || value.isEmpty())
                        continue;
                    if (property.startsWith("name") || property.startsWith("title")) {
                        final List<String> idxList = (List<String>) index.computeIfAbsent(value, x -> new ArrayList<String>());
                        if (!idxList.contains("bdr:"+mainId)) {
                            idxList.add("bdr:"+mainId);                            
                        }
                        TransferHelpers.logger.debug("adding {} -> {} to index", value, mainId);
                    }
                    if (property.endsWith("[]")) {
                        property = property.substring(0, property.length()-2);
                        final List<String> valList = (List<String>) res.computeIfAbsent(property, x -> new ArrayList<String>());
                        if (!valList.contains(value))
                            valList.add(value);
                    } else {
                        res.put(property, value);
                    }
                }
            }
        }
        if (res.isEmpty()) {
            //System.out.println("res for "+mainId+" is empty!");
            return null;
        }
        return res;
    }
    
    public static void exportAll() throws IOException {
        File exportDir = new File(GitToDB.libOutputDir);
        exportDir.mkdir();
        
        Map<String,Map<String,List<String>>> indexes = new HashMap<>();
        indexes.put(PERSON, new HashMap<>());
        indexes.put(WORKPARTS, new HashMap<>());
        indexes.put(WORK, new HashMap<>());
        
        TreeWalk tw = GitHelpers.listRepositoryContents(DocType.PERSON);
        TransferHelpers.logger.info("exporting all person files to app");
        File personsDir = new File(GitToDB.libOutputDir+"/persons/");
        personsDir.mkdir();
        new File(GitToDB.libOutputDir+"/persons/bdr/").mkdir();
        try {
            while (tw.next()) {
                final String mainId = TransferHelpers.mainIdFromPath(tw.getPathString(), DocType.PERSON);
                if (mainId == null)
                    return;
                String fullPath = GitToDB.gitDir+"persons/"+tw.getPathString();
                //TransferHelpers.logger.info("reading {}", fullPath);
                Model model = TransferHelpers.modelFromPath(fullPath, DocType.PERSON, mainId);
                Map<String, Object> obj = modelToJsonObject(mainId, model, DocType.PERSON, indexes.get(PERSON));
                if (obj != null)
                    om.writer().writeValue(new File(personsDir+"/bdr/"+mainId+".json"), obj);
                //break;
            }
        } catch (IOException e) {
            TransferHelpers.logger.error("", e);
        }
        
        Map<String,Map<String, Object>> works = new HashMap<>();
        
        tw = GitHelpers.listRepositoryContents(DocType.ITEM);
        TransferHelpers.logger.info("getting all item files for app");
        try {
            while (tw.next()) {
                final String mainId = TransferHelpers.mainIdFromPath(tw.getPathString(), DocType.ITEM);
                if (mainId == null)
                    return;
                String fullPath = GitToDB.gitDir+"items/"+tw.getPathString();
                //TransferHelpers.logger.info("reading {}", fullPath);
                Model model = TransferHelpers.modelFromPath(fullPath, DocType.ITEM, mainId);
                Map<String, Object> obj = modelToJsonObject(mainId, model, DocType.ITEM, null);
                if (obj != null) {
                    String forWorkURI = (String) obj.get("forWorkURI");
                    if (forWorkURI != null) {
                        obj.remove("forWorkURI");
                        works.put(forWorkURI, obj);                        
                    } else {
                        TransferHelpers.logger.error("item id {} did not return forWork value", mainId);
                    }
                }
            }
        } catch (IOException e) {
            TransferHelpers.logger.error("", e);
        }

        File worksDir = new File(GitToDB.libOutputDir+"/works/");
        new File(GitToDB.libOutputDir+"/works/bdr/").mkdir();
        worksDir.mkdir();
        File outlinesDir = new File(GitToDB.libOutputDir+"/workparts/");
        outlinesDir.mkdir();
        new File(GitToDB.libOutputDir+"/workparts/bdr/").mkdir();
        tw = GitHelpers.listRepositoryContents(DocType.WORK);
        TransferHelpers.logger.info("getting all works files for app");
        try {
            int i = 0;
            while (tw.next()) {
                final String mainId = TransferHelpers.mainIdFromPath(tw.getPathString(), DocType.WORK);
                if (mainId == null)
                    return;
                // we only want works with images:
                if (!works.containsKey("bdr:"+mainId)) {
                    continue;
                }
                String fullPath = GitToDB.gitDir+"works/"+tw.getPathString();
                //TransferHelpers.logger.info("reading {}", fullPath);
                Model model = TransferHelpers.modelFromPath(fullPath, DocType.WORK, mainId);
                Map<String, Object> obj = modelToJsonObject(mainId, model, DocType.WORK, indexes.get(WORK));
                if (obj == null)
                    continue;
                //obj.putAll(works.get("bdr:"+mainId));
                Map<String, Object> outlineObj = modelToOutline(mainId, model, indexes.get(WORKPARTS));
                if (outlineObj != null) {
                    om.writer().writeValue(new File(outlinesDir+"/bdr/"+mainId+".json"), outlineObj);
                    obj.put("hasParts", true);
                }
                om.writer().writeValue(new File(worksDir+"/bdr/"+mainId+".json"), obj);
                works.remove("bdr:"+mainId);
//                i += 1;
//                if (i > 3) {
//                    break;
//                }
            }
        } catch (IOException e) {
            TransferHelpers.logger.error("", e);
        }
        TransferHelpers.logger.info("writing indexes");
        for (Map.Entry<String,Map<String,List<String>>> e : indexes.entrySet()) {
            int fileCnt = 0;
            String prefix = e.getKey();
            TransferHelpers.logger.info("writing indexes of {}", prefix);
            FileWriter outfile = new FileWriter(GitToDB.libOutputDir+"/"+prefix+"-"+fileCnt+".json");
            int keyCnt = 0;
            for (Map.Entry<String,List<String>> kv : e.getValue().entrySet()) {
                if (keyCnt == 0) {
                    outfile.write('{');
                } else {
                    outfile.write(',');
                }
                outfile.write(om.writeValueAsString(kv.getKey())+":"+om.writeValueAsString(kv.getValue()));
                keyCnt += 1;
                if (keyCnt > maxkeysPerIndex) {
                    fileCnt += 1;
                    outfile.write('}');
                    outfile.flush();
                    outfile.close();
                    outfile = new FileWriter(GitToDB.libOutputDir+"/"+prefix+"-"+fileCnt+".json");
                    keyCnt = 0;
                }
            }
            if (keyCnt != 0) {
                outfile.write('}');
            }
            outfile.flush();
            outfile.close();
        }
    }
    
}
