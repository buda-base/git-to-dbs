package io.bdrc.fusekicouchdb;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.reasoner.rulesys.Rule.Parser;
import org.apache.jena.reasoner.rulesys.Rule.ParserException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.ReasonerVocabulary;

// call BDRReasoner to get a reasoner to apply to an individual graph of BDRC data
public class BDRCReasoner {
	
	// true to infer symetric properties in the same graph (?a :hasBrother ?b -> ?b :hasBrother ?a)
	public static boolean inferSymetry = false;
	
	public static final String BDO = TransferHelpers.CORE_PREFIX;
	
	// node structure 
	public static class TaxTreeNode {
		public TaxTreeNode parent = null;
		public List<TaxTreeNode> children = new ArrayList<>();
		public final String uri;
		public Boolean isLeave = false;
		public Boolean ruleToParentDone = false;
		
		public TaxTreeNode(String uri) {
			this.uri = uri;
		}
		
		public String toString() {
			return "uri: "+uri+", parent: "+(parent!=null)+", children: "+children.size()+", isLeave: "+isLeave+", ruleToParentDone: "+ruleToParentDone;
		}
	}
	
	// tag nodes of the tree having all children with ruleToParentDone to isLeave
	public static boolean tagLeaves(Map<String,TaxTreeNode> uriToTreeNode) {
		boolean taggedLeaves = false;
		mainloop:
		for (TaxTreeNode n : uriToTreeNode.values()) {
			if (n.ruleToParentDone || n.parent == null) {
				n.isLeave = false;
				continue;
			}
			// if any child is not completed, we don't tag
			for (TaxTreeNode child : n.children) {
				if (!child.ruleToParentDone)
					continue mainloop;
			}
			n.isLeave = true;
			taggedLeaves = true;
		}
		return taggedLeaves;
	}
	
	public static List<String> getSubClassofUris(final OntModel ontoModel) {
		final List<String> subClassOfUris = new ArrayList<>();
		final ExtendedIterator<Individual> it = ontoModel.listIndividuals(ontoModel.getResource(BDO+"Taxonomy"));
		while (it.hasNext()) {
			final Individual ind = it.next();
			final RDFNode n = ind.getPropertyValue(ontoModel.getProperty(BDO, "taxSubclassRelation")); // mind the case
			if (n != null && n.isURIResource()) {
				subClassOfUris.add(n.asResource().getURI());
			}
		}
		return subClassOfUris;
	}
	
	public static List<Rule> getTaxonomyRules(final OntModel ontoModel) {
		final List<String> subClassOfUris = getSubClassofUris(ontoModel);
		final List<Rule> res = new ArrayList<>();
		for (String propUri : subClassOfUris) {
			res.addAll(getTaxonomyRules(ontoModel, propUri));
		}
		return res;
	}
	
	public static List<Rule> getTaxonomyRules(final OntModel ontoModel, final String propUri) {
		int i = 0;
		final List<Rule> res = new ArrayList<Rule>();
		final Map<String,TaxTreeNode> uriToTreeNode = new HashMap<>();
		final StmtIterator it = ontoModel.getBaseModel().listStatements((Resource) null, ontoModel.getProperty(propUri), (RDFNode) null);
		while (it.hasNext()) {
			final Statement t = it.nextStatement();
			final String childUri = t.getSubject().getURI();
			// smart ass mode on
			final TaxTreeNode child = uriToTreeNode.computeIfAbsent(childUri, x -> new TaxTreeNode(x));
			final String parentUri = t.getObject().asResource().getURI();
			final TaxTreeNode parent = uriToTreeNode.computeIfAbsent(parentUri, x -> new TaxTreeNode(x));
			child.parent = parent;
			parent.children.add(child);
				
		}
		while(tagLeaves(uriToTreeNode)) {
			// then we run the tree, starting with leaves:
			for (TaxTreeNode n : uriToTreeNode.values()) {
				if (!n.isLeave || n.parent == null)
					continue;
				final String rule = "[tax"+i+": (?a ?p "+n.uri+") -> (?a ?p "+n.parent.uri+")] ";
				res.add(Rule.parseRule(rule));
				i = i + 1;
				n.ruleToParentDone = true;
			}
		}
		return res;
	}
	
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
	        	ruleString = "[subclass"+i+": (?a "+RDF.type+" "+childString+") -> (?a "+RDF.type+" "+ancestorString+")] ";
		        res.add(Rule.parseRule(ruleString));
	        	break;
	        case "p":
	        	ruleString = "[subprop"+i+": (?a "+childString+" ?b) -> (?a "+ancestorString+" ?b)] ";
		        res.add(Rule.parseRule(ruleString));
	        	break;
	        case "s":
	        	if (inferSymetry) {
	        		ruleString = "[sym"+i+": (?a "+ancestorString+" ?b) -> (?b "+ancestorString+" ?a)] ";
	        		res.add(Rule.parseRule(ruleString));
	        	}
	        	break;
	        default:
	        	if (inferSymetry) {
		        	ruleString = "[inv"+i+": (?a "+childString+" ?b) -> (?b "+ancestorString+" ?a)] ";
			        res.add(Rule.parseRule(ruleString));
			        i++;
		        	ruleString = "[inv"+i+": (?a "+ancestorString+" ?b) -> (?b "+childString+" ?a)] ";
			        res.add(Rule.parseRule(ruleString));
	        	}
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
		if (inferSymetry) {
			addRulesFromFile("owl-schema/reasoning/kinship.rules", rules);
		}
		rules.addAll(getRulesFromModel(ontoModel));
		rules.addAll(getTaxonomyRules(ontoModel));
		Reasoner reasoner = new GenericRuleReasoner(rules);
		reasoner.setParameter(ReasonerVocabulary.PROPruleMode, "forward");
		return reasoner;
	}
	
}
