package io.bdrc.gittodbs;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.junit.Test;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class EtextMergerTest {
    @Test
    public void testMerge() throws FileNotFoundException, IOException {
        Model etext = TransferHelpers.modelFromPath("etextmerge/EtextTest-etext.ttl", DocType.ETEXT, "UT1CZ2485_001_0000");
        // the following side-effects the etext model just constructed so no need to save in a separste variable
        EtextContents.getModel("src/test/resources/etextmerge/EtextTest-str.txt", "UT1CZ2485_001_0000", etext);
        
//        etext.write(new FileWriter("/Users/chris/BUDA/NEW_MIGRATION_TESTING/EtextMergerTest-GENERATED.ttl"), "TTL");
        
        Model correctMerge = TransferHelpers.modelFromPath("etextmerge/EtextTest-etext-merged.ttl", DocType.ETEXT, "UT1CZ2485_001_0000");
        TransferHelpers.printModel(etext);
        assertTrue(etext.isIsomorphicWith(correctMerge));
    }
}
