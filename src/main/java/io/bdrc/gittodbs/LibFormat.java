package io.bdrc.gittodbs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
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
        // the following nonsense just reads 
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
    
    public static Map<String, Object> objectFromModel(Model m, DocType type) {
        Query query = getQuery(type);
        Map<String,Object> res = new HashMap<String,Object>();
        try (QueryExecution qexec = QueryExecutionFactory.create(query, m)) {
            ResultSet results = qexec.execSelect() ;
            while (results.hasNext()) {
                QuerySolution soln = results.nextSolution();
                String property = soln.get("property").asLiteral().getString();
                String value = soln.get("value").asLiteral().getString();
                res.put(property, value);
            }
        }
        return res;
    }
}
