package io.brdc.fusekicouchdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.ontology.DatatypeProperty;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.ReasonerVocabulary;

public class BDRCReasoner {

	public static List<Rule> getRulesFromModel(OntModel ontoModel) {
		List<Rule> res = new ArrayList<Rule>();
		
	    String queryString = "PREFIX root: <"+TransferHelpers.ROOT_PREFIX+">\n"
	    		+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
	    		+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
	    		+ "SELECT distinct ?ancestor ?child ?type\n"
	    		+ "WHERE {\n"
	    		+ "  {\n"
	    		+ "  	?ancestor root:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subPropertyOf+ ?ancestor .\n"
	    		+ "     BIND (\"p\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "     ?grandancestor root:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subPropertyOf+ ?ancestor .\n"
	    		+ "  	?ancestor rdfs:subPropertyOf+ ?grandancestor .\n"
	    		+ "     BIND (\"p\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "  	?ancestor root:inferSubTree \"true\"^^xsd:boolean .\n"
	    		+ "  	?child rdfs:subClassOf+ ?ancestor .\n"
	    		+ "     BIND (\"c\" AS ?type)\n"
	    		+ "  } UNION {\n"
	    		+ "     ?grandancestor root:inferSubTree \"true\"^^xsd:boolean .\n"
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
	        boolean isClass = soln.get("type").asLiteral().getString().equals("c");
	        String ruleString;
	        if (isClass)
	        	ruleString = "[r"+i+": (?a "+RDF.type+" "+childString+") -> (?a "+RDF.type+" "+ancestorString+")] ";
	        else
	        	ruleString = "[r"+i+": (?a "+childString+" ?b) -> (?a "+ancestorString+" ?b)] ";
	        Rule r = Rule.parseRule(ruleString);
	        res.add(r);
	      }
	    }
		return res;
	}
	
	public static Reasoner getReasoner(OntModel ontoModel) {
		List<Rule> rules = getRulesFromModel(ontoModel);
		Reasoner reasoner = new GenericRuleReasoner(rules);
		reasoner.setParameter(ReasonerVocabulary.PROPruleMode, "forward");
		return reasoner;
	}
	
}
