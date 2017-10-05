package io.bdrc.gittodbs;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import static org.hamcrest.Matchers.*;

import org.apache.jena.rdf.model.Model;
import org.junit.Test;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class EtextMergerTest {
    @Test
    public void testMerge() throws FileNotFoundException {
        String s = "\n\nB\nBAA\nBA\n\n";
        EtextContents.EtextStrInfo esi = EtextContents.getInfos(s);
        assertTrue("BBAABA".equals(esi.totalString));
        assertThat(esi.breakList, contains(0,0,1, 4,6));
        InputStream is = new ByteArrayInputStream(s.getBytes());
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
        esi = EtextContents.getInfos(r);
        assertTrue("BBAABA".equals(esi.totalString));
        assertThat(esi.breakList, contains(0,0,1, 4,6));
        Model etext = TransferHelpers.modelFromPath("etextmerge/EtextTest-etext.ttl", DocType.ETEXT, "UT1CZ2485_001_0000");
        Model contents = EtextContents.getModel("UT1CZ2485_001_0000", new BufferedReader(new FileReader(new File("src/test/resources/etextmerge/EtextTest-str.txt"))));
        etext.add(contents);
        Model correctMerge = TransferHelpers.modelFromPath("etextmerge/EtextTest-etext-merged.ttl", DocType.ETEXT, "UT1CZ2485_001_0000");
        //TransferHelpers.printModel(etext);
        assertTrue(etext.isIsomorphicWith(correctMerge));
    }
}
