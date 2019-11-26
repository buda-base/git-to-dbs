package io.bdrc.gittodbs;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class LibFormatTest {
    
    public static final ObjectMapper om = new ObjectMapper();
    
    @Test
    public void allTest() throws JsonGenerationException, JsonMappingException, IOException {
        Model model = TransferHelpers.modelFromPath("src/test/resources/P1583.ttl", DocType.PERSON, "P1583");
        Map<String,List<String>> personidx = new HashMap<>();
        Map<String, Object> jsonres = LibFormat.modelToJsonObject("P1583", model, DocType.PERSON, personidx);
        //om.writerWithDefaultPrettyPrinter().writeValue(System.out, jsonres);
        //om.writerWithDefaultPrettyPrinter().writeValue(System.out, personidx);
        model = TransferHelpers.modelFromPath("src/test/resources/WorkTestFPL.ttl", DocType.WORK, "W12837FPL");
        Map<String,List<String>> workidx = new HashMap<>();
        jsonres = LibFormat.modelToJsonObject("W12837FPL", model, DocType.WORK, workidx);
        //om.writerWithDefaultPrettyPrinter().writeValue(System.out, jsonres);
        model = TransferHelpers.modelFromPath("src/test/resources/OutlineTest.ttl", DocType.WORK, "W30020");
        Map<String,List<String>> workoutlineidx = new HashMap<>();
        jsonres = LibFormat.modelToOutline("W30020", model, workoutlineidx);
        om.writerWithDefaultPrettyPrinter().writeValue(System.out, jsonres);
    }
    
}
