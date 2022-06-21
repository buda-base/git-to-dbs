package io.bdrc.gittodbs;

import static org.junit.Assert.assertTrue;

import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.ontology.OntDocumentManager;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Reasoning {

    public static Model ontModel;
    public static Reasoner reasoner;
    private static final String ADM = TransferHelpers.ADM;
    private static final String BDR = TransferHelpers.BDR;
    private static final String BDO = TransferHelpers.BDO;
    
    // unfortunately this test requires the ontology files to be run, see the init() function class
    // so it is not run automatically
    
    
    @BeforeClass
    public static void init() {
        // change this to be able to test
        String owlSchemaBase = "/home/eroux/BUDA/softs/owl-schema/";
        OntDocumentManager ontManager = new OntDocumentManager(owlSchemaBase+"ont-policy.rdf");
        ontManager.setProcessImports(true); // not really needed since ont-policy sets it, but what if someone changes the policy
        
        OntModelSpec ontSpec = new OntModelSpec( OntModelSpec.OWL_DL_MEM );
        ontSpec.setDocumentManager( ontManager );           
        
        OntModel ontModel = ontManager.getOntology( "http://purl.bdrc.io/ontology/admin/", ontSpec );

        ontModel = TransferHelpers.getOntologyModel();
        reasoner = BDRCReasoner.getReasoner(ontModel, owlSchemaBase+"reasoning/kinship.rules", true);
    }
    
    @Test
    public void testInferSubtreeClass()  {
        Model m = ModelFactory.createDefaultModel();
        Resource r1 = m.createResource(BDR+"r1");
        m.add(r1, RDF.type, m.createResource(BDO+"ImageInstance"));
        InfModel im = ModelFactory.createInfModel(reasoner, m);
        assertTrue(im.contains(r1, RDF.type, m.createResource(BDO+"Instance")));
    }
    
    @Test
    public void testInferSubtreeProp()  {
        Model m = ModelFactory.createDefaultModel();
        Resource r1 = m.createResource(BDR+"r1");
        Resource r2 = m.createResource(BDR+"r2");
        Property incarnationActivities = m.createProperty(BDO, "incarnationActivities");
        Property isIncarnation = m.createProperty(BDO, "isIncarnation");
        m.add(r1, incarnationActivities, r2);
        // with annotations
        Property sameAsrKTs = m.createProperty(ADM, "sameAsrKTs");
        m.add(r1, sameAsrKTs, r2);
        InfModel im = ModelFactory.createInfModel(reasoner, m);
        assertTrue(im.contains(r1, isIncarnation, r2));
        assertTrue(im.contains(r1, OWL.sameAs, r2));
    }
    
    @Test
    public void testInverse()  {
        Model m = ModelFactory.createDefaultModel();
        Resource r1 = m.createResource(BDR+"r1");
        Resource r2 = m.createResource(BDR+"r2");
        Property studentOf = m.createProperty(BDO, "personStudentOf");
        Property teacherOf = m.createProperty(BDO, "personTeacherOf");
        m.add(r1, studentOf, r2);
        InfModel im = ModelFactory.createInfModel(reasoner, m);
        assertTrue(im.contains(r2, teacherOf, r1));
    }

    @Test
    public void testKin() {
        Model m = ModelFactory.createDefaultModel();
        Resource r1 = m.createResource(BDR+"r1");
        Resource r2 = m.createResource(BDR+"r2");
        m.add(r1, m.getProperty(BDO, "personGender"), m.createResource(BDR+"GenderFemale"));
        m.add(r2, m.getProperty(BDO, "personGender"), m.createResource(BDR+"GenderMale"));
        m.add(r1, m.createProperty(BDO, "hasOlderBrother"), r2);
        InfModel im = ModelFactory.createInfModel(reasoner, m);
        assertTrue(im.contains(r1, m.createProperty(BDO, "hasBrother"), r2));
        assertTrue(im.contains(r1, m.createProperty(BDO, "hasSibling"), r2));
        assertTrue(im.contains(r1, m.createProperty(BDO, "kinWith"), r2));
        assertTrue(im.contains(r2, m.createProperty(BDO, "kinWith"), r1));
        assertTrue(im.contains(r2, m.createProperty(BDO, "hasSibling"), r1));
        assertTrue(im.contains(r2, m.createProperty(BDO, "hasSister"), r1));
        assertTrue(im.contains(r2, m.createProperty(BDO, "hasYoungerSister"), r1));
    }

    @Test
    public void testSymmetry()  {
        Model m = ModelFactory.createDefaultModel();
        Resource r1 = m.createResource(BDR+"r1");
        Resource r2 = m.createResource(BDR+"r2");
        Property kinWith = m.createProperty(BDO, "kinWith");
        m.add(r1, kinWith, r2);
        InfModel im = ModelFactory.createInfModel(reasoner, m);
        assertTrue(im.contains(r2, kinWith, r1));
    }
    
    public static final class EDTFStr {
        public String str = null;
        
        public EDTFStr(String edtfstr) {
            this.str = edtfstr;
        }
        
        @Override
        public boolean equals(Object other) {
            if (other instanceof EDTFStr) {
                return this.str.equals(((EDTFStr)other).str);
            } else {
                return false;
            }
        }
    }
    
    static final RDFDatatype EDTFDT = new BaseDatatype("http://id.loc.gov/datatypes/edtf") {
        @Override
        public Class getJavaClass() {
            return EDTFStr.class;
        }

        @Override
        public String unparse(Object value) {
            return ((EDTFStr)value).str;
        }

        @Override
        public EDTFStr parse(String lexicalForm) throws DatatypeFormatException {
            return new EDTFStr(lexicalForm);
        }
        
        @Override
        public boolean isEqual(LiteralLabel value1, LiteralLabel value2) {
            return value1.getDatatypeURI().equals(value2.getDatatypeURI()) && value1.getLexicalForm().equals(value2.getLexicalForm());
        }
    };
    
    @Test
    public void testEDTF()  {
        Model m = ModelFactory.createDefaultModel();
        m.add(m.createResource(BDR+"r1"), BDRCReasoner.eventWhen, m.createTypedLiteral("0123", EDTFDT));
        m.add(m.createResource(BDR+"r2"), BDRCReasoner.eventWhen, m.createTypedLiteral("0123?", EDTFDT));
        m.add(m.createResource(BDR+"r3"), BDRCReasoner.eventWhen, m.createTypedLiteral("012X", EDTFDT));
        m.add(m.createResource(BDR+"r4"), BDRCReasoner.eventWhen, m.createTypedLiteral("012X?", EDTFDT));
        m.add(m.createResource(BDR+"r5"), BDRCReasoner.eventWhen, m.createTypedLiteral("012X/", EDTFDT));
        m.add(m.createResource(BDR+"r6"), BDRCReasoner.eventWhen, m.createTypedLiteral("0123/", EDTFDT));
        m.add(m.createResource(BDR+"r7"), BDRCReasoner.eventWhen, m.createTypedLiteral("/0123", EDTFDT));
        m.add(m.createResource(BDR+"r8"), BDRCReasoner.eventWhen, m.createTypedLiteral("0123/0126", EDTFDT));
        m.add(m.createResource(BDR+"r9"), BDRCReasoner.eventWhen, m.createTypedLiteral("011X/0126", EDTFDT));
        m.add(m.createResource(BDR+"r10"), BDRCReasoner.eventWhen, m.createTypedLiteral("011X?/0126", EDTFDT));
        m.add(m.createResource(BDR+"r11"), BDRCReasoner.eventWhen, m.createTypedLiteral("[0123, 0124]", EDTFDT));
        BDRCReasoner.addFromEDTF(m);
        m.write(System.out, "TTL");
    }
    
}
