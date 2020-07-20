package io.bdrc.gittodbs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.reasoner.rulesys.Rule.Parser;
import org.apache.jena.reasoner.rulesys.Rule.ParserException;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.ReasonerVocabulary;

import io.bdrc.libraries.Models;

// call BDRReasoner to get a reasoner to apply to an individual graph of BDRC data
public class BDRCReasoner {

    public static final String BDO = Models.BDO;

    private static List<Rule> getRulesFromModel(Model m, boolean inferSymetry) {
        List<Rule> res = new ArrayList<Rule>();

        String queryString = "PREFIX bdo: <" + Models.BDO + ">\n" + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
                + "SELECT distinct ?ancestor ?child ?type\n" + "WHERE {\n" + "  {\n" + "     ?child owl:inverseOf ?ancestor .\n" + "     BIND (\"i\" AS ?type)\n" + "  } UNION {\n" + "     ?ancestor a owl:SymmetricProperty .\n"
                + "     BIND (\"s\" AS ?type).\n" + "     BIND (?ancestor AS ?child)\n" + "  } UNION {\n" + "     ?ancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n" + "     ?child rdfs:subPropertyOf+ ?ancestor .\n" + "     BIND (\"p\" AS ?type)\n"
                + "  } UNION {\n" + "     ?grandancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n" + "     ?child rdfs:subPropertyOf+ ?ancestor .\n" + "     ?ancestor rdfs:subPropertyOf+ ?grandancestor .\n" + "     BIND (\"p\" AS ?type)\n"
                + "  } UNION {\n" + "     ?ancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n" + "     ?child rdfs:subClassOf+ ?ancestor .\n" + "     BIND (\"c\" AS ?type)\n" + "  } UNION {\n"
                + "     ?grandancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n" + "     ?child rdfs:subClassOf+ ?ancestor .\n" + "     ?ancestor rdfs:subClassOf+ ?grandancestor .\n" + "     BIND (\"c\" AS ?type)\n" + "  }\n" + "}\n";
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, m)) {
            ResultSet results = qexec.execSelect();
            for (int i = 0; results.hasNext(); i++) {
                QuerySolution soln = results.nextSolution();
                String ancestorString = soln.get("ancestor").asResource().getURI();
                String childString = soln.get("child").asResource().getURI();
                String type = soln.get("type").asLiteral().getString();
                String ruleString;
                switch (type) {
                case "c":
                    ruleString = "[subclass" + i + ": (?a " + RDF.type + " " + childString + ") -> (?a " + RDF.type + " " + ancestorString + ")] ";
                    res.add(Rule.parseRule(ruleString));
                    break;
                case "p":
                    ruleString = "[subprop" + i + ": (?a " + childString + " ?b) -> (?a " + ancestorString + " ?b)] ";
                    res.add(Rule.parseRule(ruleString));
                    break;
                case "s":
                    if (inferSymetry) {
                        ruleString = "[sym" + i + ": (?a " + ancestorString + " ?b) -> (?b " + ancestorString + " ?a)] ";
                        res.add(Rule.parseRule(ruleString));
                    }
                    break;
                default:
                    if (inferSymetry) {
                        ruleString = "[inv" + i + ": (?a " + childString + " ?b) -> (?b " + ancestorString + " ?a)] ";
                        res.add(Rule.parseRule(ruleString));
                        i++;
                        ruleString = "[inv" + i + ": (?a " + ancestorString + " ?b) -> (?b " + childString + " ?a)] ";
                        res.add(Rule.parseRule(ruleString));
                    }
                    break;
                }
            }
        }
        return res;
    }

    private static void addRulesFromSource(String filePath, List<Rule> rules) {
        try {
            InputStream rulesFile = new FileInputStream(filePath);
            BufferedReader in = new BufferedReader(new InputStreamReader(rulesFile));
            Parser p = Rule.rulesParserFromReader(in);
            rules.addAll(Rule.parseRules(p));
            rulesFile.close();
        } catch (ParserException | IOException e) {
            System.err.println("error parsing " + filePath + " while trying to add rules");
            e.printStackTrace(System.err);
        }
    }

    public static Reasoner getReasoner(final Model ontModel, final String rulesPath, final boolean symmetry) {
        List<Rule> rules = new ArrayList<Rule>();
        rules.addAll(getRulesFromModel(ontModel, symmetry));
        addRulesFromSource(rulesPath, rules);
        Reasoner reasoner = new GenericRuleReasoner(rules);
        reasoner.setParameter(ReasonerVocabulary.PROPruleMode, "forward");
        return reasoner;
    }
    
    public static Model getUnreasonable(final Model ontModel, final Model m) {
        Dataset union = DatasetFactory.create();
        union.addNamedModel("http://example.com/ont", ontModel);
        union.addNamedModel("http://example.com/other", m);
        String queryString = "PREFIX bdo: <" + Models.BDO + ">\n" + "PREFIX rdfs: <"+RDFS.uri+">\n"+ "PREFIX rdf: <"+RDF.uri+">\n"
                + "CONSTRUCT {?s ?p ?o .} WHERE {\n" 
                + "  {\n" 
                + "     graph <http://example.com/ont> { ?subclass rdfs:subClassOf+ ?o . } graph <http://example.com/other> { ?s rdf:type ?subclass, ?o . } BIND(rdf:type as ?p) \n" 
                + "  } UNION {\n" 
                + "     graph <http://example.com/ont> { ?subprop rdfs:subPropertyOf+ ?p . } graph <http://example.com/other> { ?s ?subprop ?o ; ?p ?o . } \n"
                + "  }\n" 
                + "}\n";
        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, union);
        return qexec.execConstruct();
    }

}
