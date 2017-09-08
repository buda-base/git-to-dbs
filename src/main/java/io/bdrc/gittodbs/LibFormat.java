package io.bdrc.gittodbs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.reasoner.Reasoner;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.ewtsconverter.EwtsConverter;
import io.bdrc.gittodbs.TransferHelpers.DocType;

public class LibFormat {
    public static final ObjectMapper om = new ObjectMapper();
    public static final EwtsConverter converter = new EwtsConverter(false, false, false, true);
    public static final Reasoner bdrcReasoner = TransferHelpers.bdrcReasoner;

    public static final Map<DocType,Query> typeQuery = new EnumMap<>(DocType.class);
    
    public static Query getQuery(DocType type) {
        if (typeQuery.containsKey(type))
            return typeQuery.get(type);
        // the following nonsense just reads a freaking file
        ClassLoader classLoader = TransferHelpers.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("sparql/"+TransferHelpers.typeToStr.get(type)+".sparql");
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
    
    public static String uriToShort(final String uri) {
        return uri.substring(TransferHelpers.BDR.length());
    }
    
    public static String getUnicodeStrFromProp(QuerySolution soln, String var) {
        RDFNode valueN = soln.get(var);
        if (valueN == null)
            return null;
        Literal valueL = valueN.asLiteral();
        if (valueL.getLanguage().equals("bo-x-ewts"))
            return toUnicode(valueL.getString());
        else
            return valueL.getString();
    }
    
    public static Map<String, Object> objectFromModel(Model m, DocType type) {
        Query query = getQuery(type);
        InfModel im = TransferHelpers.getInferredModel(m);
        //TransferHelpers.printModel(im);
        Map<String,Object> res = new HashMap<String,Object>();
        try (QueryExecution qexec = QueryExecutionFactory.create(query, im)) {
            ResultSet results = qexec.execSelect() ;
            while (results.hasNext()) {
                QuerySolution soln = results.nextSolution();
                String property = soln.get("property").asLiteral().getString();
                if (property.equals("node[]")) {
                    System.out.println(soln.toString());
                    String value = getUnicodeStrFromProp(soln, "value");
                    if (value == null || value.isEmpty())
                        continue;
                } 
                else if (property.contains("URI")) {
                    Resource valueURI = soln.get("value").asResource();
                    String value = uriToShort(valueURI.getURI());
                    if (property.endsWith("[URI]")) {
                        property = property.substring(0, property.length()-5);
                        List<String> valList = (List<String>) res.computeIfAbsent(property, x -> new ArrayList<String>());
                        valList.add(value);
                    } else {
                        property = property.substring(0, property.length()-3);
                        res.put(property, value);
                    }
                } else {
                    String value = getUnicodeStrFromProp(soln, "value");
                    if (value == null || value.isEmpty())
                        continue;
                    if (property.endsWith("[]")) {
                        property = property.substring(0, property.length()-2);
                        List<String> valList = (List<String>) res.computeIfAbsent(property, x -> new ArrayList<String>());
                        valList.add(value);
                    } else {
                        res.put(property, value);                    
                    }
                }
            }
        }
        return res;
    }
}
