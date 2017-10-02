package io.bdrc.gittodbs;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.junit.Test;

public class EtextMergerTest {
    @Test
    public void testMerge() throws FileNotFoundException {
        String s = "\n\nB\nBAA\nBA\n\n";
        EtextMerger.EtextStrInfo esi = EtextMerger.getInfos(s);
        System.out.println(esi.totalString);
        System.out.println(esi.breakList);
        InputStream is = new ByteArrayInputStream(s.getBytes());
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
        esi = EtextMerger.getInfos(r);
        System.out.println(esi.totalString);
        System.out.println(esi.breakList);
//        r = new BufferedReader(new FileReader(new File("etextmerge/EtextTest-str.txt")));
//        esi = EtextMerger.getInfos(r);

    }
}
