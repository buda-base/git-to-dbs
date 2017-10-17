package io.bdrc.gittodbs;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.eclipse.jgit.treewalk.TreeWalk;

import com.opencsv.CSVWriter;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class CSVExport {

    public static CSVWriter writer;
    public static Query query;
    public static final Map<String,Integer> propToColNum = new HashMap<>();
    static {
        init();
    }
    
    public static void init() {
        try {
            writer = new CSVWriter(new FileWriter("output.csv"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        final String[] titles = new String[12];
        titles[0] = "Work RID";
        titles[1] = "Creators RIDs";
        titles[2] = "Work Titles"; // 
        titles[3] = "Work Restriction"; // access
        titles[4] = "Publisher Name"; // workPublisherName
        titles[5] = "Publisher Location"; // workPublisherLocation
        titles[6] = "Printery"; // workPrintery
        titles[7] = "Publishing Date"; // publisherDate
        titles[8] = "Edition Statement"; // workEditionStatement
        titles[9] = "Authorship Statement"; // workAuthorshipStatement
        titles[10] = "Bibliographic Notes"; // workBiblioNote
        titles[11] = "Source Notes"; // workSourceNote
        writer.writeNext(titles);
        propToColNum.put("creator[URI]", 1);
        propToColNum.put("title[]", 2);
        propToColNum.put("accessURI", 3);
        propToColNum.put("workPublisherName", 4);
        propToColNum.put("workPublisherLocation", 5);
        propToColNum.put("workPrintery", 6);
        propToColNum.put("workPublisherDate", 7);
        propToColNum.put("workEditionStatement", 8);
        propToColNum.put("workAuthorshipStatement", 9);
        propToColNum.put("workBiblioNote", 10);
        propToColNum.put("workSourceNote", 11);
        ClassLoader classLoader = TransferHelpers.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("sparql/csvexport.sparql");
        Scanner s = new Scanner(inputStream);
        s.useDelimiter("\\A");
        String result = s.hasNext() ? s.next() : "";
        s.close();
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        query = QueryFactory.create(result.toString());
    }
    
    public static void export() {
        final DocType type = DocType.WORK;
        final String dirpath = GitToDB.gitDir+TransferHelpers.typeToStr.get(type)+"s/";
        TreeWalk tw = GitHelpers.listRepositoryContents(type);
        try {
            while (tw.next()) {
                final String filePath = tw.getPathString();
                final String mainId = TransferHelpers.mainIdFromPath(filePath, type);
                if (mainId == null || mainId.contains("FPL"))
                    continue;
                Model m = TransferHelpers.modelFromPath(dirpath+filePath, type, mainId);
                handleModel(m, mainId);
            }
        } catch (IOException e) {
            TransferHelpers.logger.error("", e);
        } finally {
            close();
        }
        
    }
    
    public static void handleModel(Model m, String mainRID) {
        final String[] res = new String[] { mainRID, "", "", "", "", "", "", "", "", "", "", ""};
        final InfModel im = TransferHelpers.getInferredModel(m);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, im)) {
            final ResultSet results = qexec.execSelect();
            while (results.hasNext()) {
                final QuerySolution soln = results.nextSolution();
                if (!soln.contains("property"))
                    continue;
                String property = soln.get("property").asLiteral().getString();
                final Integer colNum = propToColNum.get(property);
                if (colNum == null) {
                    System.err.println("cannot find "+property);
                }
                if (property.contains("URI")) {
                    final Resource valueURI = soln.get("value").asResource();
                    final String value = valueURI.getLocalName();
                    if (!res[colNum].isEmpty())
                        res[colNum] += ", ";
                    res[colNum] += value;
                } else {
                    final String value = soln.get("value").asLiteral().getString();
                    if (value == null || value.isEmpty())
                        continue;
                    if (!res[colNum].isEmpty())
                        res[colNum] += ", ";
                    res[colNum] += value;
                }
            }
        }
        writer.writeNext(res);
    }
    
    public static void close() {
        try {
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
