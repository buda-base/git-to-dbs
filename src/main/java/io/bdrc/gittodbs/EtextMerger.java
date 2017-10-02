package io.bdrc.gittodbs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class EtextMerger {

    public static int meanChunkPointsAim = 600;
    public static int maxChunkPointsAim = 1200;
    public static int minChunkNbSylls = 6;
    
    public static class EtextStrInfo {
        public String totalString;
        public List<Integer> breakList;
        public EtextStrInfo(String totalString, List<Integer> breakList) {
            this.totalString = totalString;
            this.breakList = breakList;
        }
    }
    
    public static String translatePoint(final List<Integer> pointBreaks, final int pointIndex, final boolean isEnd) {
        // pointIndex depends on the context, 
        // if it's about the starting point (isEnd == false):
        //     it's the index of the starting char: ab|cd -> pointIndex 2 for the beginning of the second segment (cd)
        // else 
        //     it's the index after the end char: ab|cd -> pointIndex 2 for the end of the first segment (ab)
        int curLine = 1;
        int toSubstract = 0;
        for (final int pointBreak : pointBreaks) {
            // pointBreak is the index at which the break occurs, for instance
            // a|bc|d will have pointBreaks of 1 and 3
            if (pointBreak > pointIndex || (isEnd && pointBreak == pointIndex)) {
                break;
            }
            toSubstract = pointBreak;
            curLine += 1;
        }
        return curLine+"-"+(pointIndex-toSubstract+1); // +1 so that characters start at 1
    }
    
    public static EtextStrInfo getInfos(final String origString) {
        final List<Integer> breakList = new ArrayList<Integer>();
        final StringBuilder sb = new StringBuilder();
        int lastCharIndex = 0;
        int currentTransformedPointIndex = 0;
        final int origCharLength = origString.length();
        for (int charIndex = origString.indexOf('\n');
                charIndex >= 0;
                charIndex = origString.indexOf('\n', charIndex + 1))
           {
               final String line = origString.substring(lastCharIndex, charIndex);
               lastCharIndex = charIndex+1;
               final int lineLenPoints = line.codePointCount(0, line.length());
               sb.append(line);
               currentTransformedPointIndex += lineLenPoints;
               breakList.add(currentTransformedPointIndex);
               
           }
        sb.append(origString.substring(lastCharIndex, origCharLength));
        // case of a final \n
        if (lastCharIndex == origCharLength) {
            breakList.remove(breakList.size()-1);
        }
        return new EtextStrInfo(sb.toString(), breakList);
    }
    
    public static EtextStrInfo getInfos(final BufferedReader r) {
        final List<Integer> breakList = new ArrayList<Integer>();
        final StringBuilder sb = new StringBuilder();
        int currentTransformedPointIndex = 0;
        try {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                sb.append(line);
                final int lineLenPoints = line.codePointCount(0, line.length());
                currentTransformedPointIndex += lineLenPoints;
                breakList.add(currentTransformedPointIndex);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
        breakList.remove(breakList.size()-1);
        return new EtextStrInfo(sb.toString(), breakList);
    }
    
    public static void merge(final Model m, final String etextId) {
        final String StrFilePath = GitToDB.gitDir+"etextcontents/"+TransferHelpers.getMd5(etextId)+"/"+etextId+".txt";
        BufferedReader r;
        try {
            r = new BufferedReader(new FileReader(new File(StrFilePath)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        EtextStrInfo esi = getInfos(r);
        if (esi == null)
            return;
        final List<Integer>[] tmpBreaks = TibetanStringChunker.getAllBreakingCharsIndexes(esi.totalString);
        final List<Integer>[] breaks = TibetanStringChunker.selectBreakingCharsIndexes(tmpBreaks, meanChunkPointsAim, maxChunkPointsAim, minChunkNbSylls);
        // tmpBreaks[3].get(0) is the total length in code points
        merge(m, breaks, tmpBreaks[3].get(0), esi.totalString, esi.breakList, etextId);
    }
    
    // mostly for testing purposes
    public static void merge(final Model m, final String etextId, final BufferedReader r) {
        EtextStrInfo esi = getInfos(r);
        if (esi == null)
            return;
        final List<Integer>[] tmpBreaks = TibetanStringChunker.getAllBreakingCharsIndexes(esi.totalString);
        final List<Integer>[] breaks = TibetanStringChunker.selectBreakingCharsIndexes(tmpBreaks, meanChunkPointsAim, maxChunkPointsAim, minChunkNbSylls);
        // tmpBreaks[3].get(0) is the total length in code points
        merge(m, breaks, tmpBreaks[3].get(0), esi.totalString, esi.breakList, etextId);
    }
    
    public static void merge(final Model m, final List<Integer>[] breaks, final int totalStringPointNum, final String totalString, final List<Integer> initialBreakList, final String etextId) {
        final List<Integer> charBreaks = breaks[0];
        final List<Integer> pointBreaks = breaks[1];
        int chunkSeqNum = 1;
        int lastCharBreakIndex = 0;
        int lastPointBreakIndex = 0;
        final Resource etext = m.getResource(TransferHelpers.BDR+etextId);
        final Property hasChunk = m.getProperty(TransferHelpers.BDO, "eTextHasChunk");
        final Property seqNum = m.getProperty(TransferHelpers.BDO, "seqNum");
        final Property chunkContents = m.getProperty(TransferHelpers.BDO, "chunkContents");
        final Property sliceStart = m.getProperty(TransferHelpers.BDO, "sliceStart");
        final Property sliceEnd = m.getProperty(TransferHelpers.BDO, "sliceEnd");
        for (chunkSeqNum = 1 ; chunkSeqNum <= charBreaks.size() ; chunkSeqNum++) {// final int charBreakIndex : charBreaks) {
            final int charBreakIndex = charBreaks.get(chunkSeqNum-1);
            final int pointBreakIndex = pointBreaks.get(chunkSeqNum-1);
            final String contents = totalString.substring(lastCharBreakIndex, charBreakIndex);
            Resource chunk = m.createResource();
            etext.addProperty(hasChunk, chunk);
            chunk.addProperty(seqNum, m.createTypedLiteral(chunkSeqNum, XSDDatatype.XSDinteger));
            chunk.addProperty(chunkContents, m.createLiteral(contents, "bo")); // TODO: what about multilingual etexts?
            final String start = translatePoint(initialBreakList, lastPointBreakIndex, false);
            final String end = translatePoint(initialBreakList, pointBreakIndex, true);
            chunk.addProperty(sliceStart, m.createLiteral(start));
            chunk.addProperty(sliceEnd, m.createLiteral(end));
            lastCharBreakIndex = charBreakIndex;
            lastPointBreakIndex = pointBreakIndex;
        }
        if (lastCharBreakIndex != totalString.length()) {
            final String contents = totalString.substring(lastCharBreakIndex);
            Resource chunk = m.createResource();
            etext.addProperty(hasChunk, chunk);
            chunk.addProperty(seqNum, m.createTypedLiteral(chunkSeqNum, XSDDatatype.XSDinteger));
            chunk.addProperty(chunkContents, m.createLiteral(contents, "bo"));
            final String start = translatePoint(initialBreakList, lastPointBreakIndex, false);
            final String end = translatePoint(initialBreakList, totalStringPointNum, true);
            chunk.addProperty(sliceStart, m.createLiteral(start));
            chunk.addProperty(sliceEnd, m.createLiteral(end));
        }
    }
}
