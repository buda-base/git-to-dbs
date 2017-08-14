package io.brdc.fusekicouchdb;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.reasoner.rulesys.Rule.Parser;
import org.apache.jena.reasoner.rulesys.Rule.ParserException;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.ReasonerVocabulary;

public class BDRCReasoner {

	public static List<Rule> getRulesFromModel(OntModel ontoModel) {
		List<Rule> res = new ArrayList<Rule>();
		
	    String queryString = "PREFIX bdo: <"+TransferHelpers.CORE_PREFIX+">\n"
	    		+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
	    		+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
	    		+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
	    		+ "SELECT distinct ?ancestor ?child ?type\n"
	    		+ "WHERE {\n"
	    		+ "  {\n"
	    		+ "  	?child owl:inverseOf ?ancestor .\n"
	    		+ "     BIND (\"i\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "  	?ancestor a owl:SymmetricProperty .\n"
	    		+ "     BIND (\"s\" AS ?type).\n"
	    		+ "     BIND (?ancestor AS ?child)\n"
	    		+ "  } UNION {\n"
	    		+ "  	?ancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subPropertyOf+ ?ancestor .\n"
	    		+ "     BIND (\"p\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "     ?grandancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subPropertyOf+ ?ancestor .\n"
	    		+ "  	?ancestor rdfs:subPropertyOf+ ?grandancestor .\n"
	    		+ "     BIND (\"p\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "  	?ancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subClassOf+ ?ancestor .\n"
	    		+ "     BIND (\"c\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "     ?grandancestor bdo:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subClassOf+ ?ancestor .\n"
	    		+ "  	?ancestor rdfs:subClassOf+ ?grandancestor .\n"
	    		+ "     BIND (\"c\" AS ?type)\n"
	    		+ "  }\n"
	    		+ "}\n" ;
	    Query query = QueryFactory.create(queryString) ;
	    try (QueryExecution qexec = QueryExecutionFactory.create(query, ontoModel)) {
	      ResultSet results = qexec.execSelect() ;
	      for (int i = 0 ; results.hasNext() ; i++)
	      {
	        QuerySolution soln = results.nextSolution() ;
	        String ancestorString = soln.get("ancestor").asResource().getURI();
	        String childString = soln.get("child").asResource().getURI();
	        String type = soln.get("type").asLiteral().getString();
	        String ruleString;
	        switch(type) {
	        case "c":
	        	ruleString = "[r"+i+": (?a "+RDF.type+" "+childString+") -> (?a "+RDF.type+" "+ancestorString+")] ";
		        res.add(Rule.parseRule(ruleString));
	        	break;
	        case "p":
	        	ruleString = "[r"+i+": (?a "+childString+" ?b) -> (?a "+ancestorString+" ?b)] ";
		        res.add(Rule.parseRule(ruleString));
	        	break;
	        case "s":
	        	ruleString = "[r"+i+": (?a "+ancestorString+" ?b) -> (?b "+ancestorString+" ?a)] ";
		        res.add(Rule.parseRule(ruleString));
	        	break;
	        default:
	        	ruleString = "[r"+i+": (?a "+childString+" ?b) -> (?b "+ancestorString+" ?a)] ";
		        res.add(Rule.parseRule(ruleString));
		        i++;
	        	ruleString = "[r"+i+": (?a "+ancestorString+" ?b) -> (?b "+childString+" ?a)] ";
		        res.add(Rule.parseRule(ruleString));
	        	break;
	        }
	      }
	    }
		return res;
	}
	
	public static void addRulesFromFile(String fileName, List<Rule> rules) {
		ClassLoader classLoader = BDRCReasoner.class.getClassLoader();
        InputStream rulesFile = classLoader.getResourceAsStream(fileName);
        try {
		    BufferedReader in = new BufferedReader(new InputStreamReader(rulesFile));
            Parser p = Rule.rulesParserFromReader(in);
            rules.addAll(p.getRulesPreload());
		} catch(ParserException e) {
			System.err.println("error parsing "+rulesFile.toString());
			e.printStackTrace(System.err);
		}
	}
	
	public static Reasoner getReasoner(OntModel ontoModel) {
		List<Rule> rules = new ArrayList<Rule>();
		//addRulesFromFile("owl-schema/reasoning/kinship.rules", rules);
		rules.addAll(getRulesFromModel(ontoModel));
		Reasoner reasoner = new GenericRuleReasoner(rules);
		reasoner.setParameter(ReasonerVocabulary.PROPruleMode, "forward");
		return reasoner;
	}
	
}
