package io.bdrc.gittodbs;

import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class ESTest {

    final static ObjectMapper mapper = new ObjectMapper();
    
    @BeforeClass
    public static void before() {
        GitToDB.ontRoot = "/home/eroux/BUDA/softs/owl-schema/";
        TransferHelpers.ontModel = TransferHelpers.getOntologyModel();
    }
    
    public static void assert_trig_to_json(final String path, final DocType type, final String graph_lname, final String ref_json_path) throws StreamReadException, DatabindException, IOException {
        final Model m = TransferHelpers.modelFromPath(path, type, graph_lname);
        //final ObjectNode ref = mapper.readValue(Paths.get(ref_json_path).toFile(), ObjectNode.class);
        ObjectNode root = ESUtils.om.createObjectNode();
        ESUtils.addModelToESDoc(m, root, graph_lname, true, true, true);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
        //assert(ref.equals(root));
    }
    
    @Test
    public void test1() throws StreamReadException, DatabindException, IOException {
        assert_trig_to_json("src/test/resources/ES/P1583.trig", DocType.PERSON, "P1583", "src/test/resources/ES/P1583.json");
    }
}
