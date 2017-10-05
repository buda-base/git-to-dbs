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
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class EtextContents {

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
    
    public static final boolean LOC_START = true;
    public static final boolean LOC_END = false;
    public static void addChunkLocation(Model m, Resource r, int chunkNum, int charNum, boolean start) {
        final String startEndString = (start ? "Start" : "End");
        r.addProperty(m.getProperty(TransferHelpers.BDO, "slice"+startEndString+"Chunk"), m.createTypedLiteral(chunkNum, XSDDatatype.XSDinteger));
        r.addProperty(m.getProperty(TransferHelpers.BDO, "slice"+startEndString+"Char"), m.createTypedLiteral(charNum, XSDDatatype.XSDinteger));
    }
    
    public static int[] translatePoint(final List<Integer> pointBreaks, final int pointIndex, final boolean isStart) {
        // pointIndex depends on the context, 
        // if it's about the starting point (isStart == true):
        //     it's the index of the starting char: ab|cd -> pointIndex 2 for the beginning of the second segment (cd)
        // else 
        //     it's the index after the end char: ab|cd -> pointIndex 2 for the end of the first segment (ab)
        int curLine = 1;
        int toSubstract = 0;
        for (final int pointBreak : pointBreaks) {
            // pointBreak is the index of the char after which the break occurs, for instance
            // a|bc|d will have pointBreaks of 1 and 3
            if (pointBreak >= pointIndex) {
                break;
            }
            toSubstract = pointBreak;
            curLine += 1;
        }
        return new int[] {curLine, pointIndex-toSubstract};
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
    
    public static Model getModel(final String etextId) {
        final String filePath = GitToDB.gitDir+"etextcontents/"+TransferHelpers.getMd5(etextId)+"/"+etextId+".txt";
        return getModel(filePath, etextId);
    }
    
    public static Model getModel(final String filePath, final String etextId) {
        final BufferedReader r;
        try {
            r = new BufferedReader(new FileReader(new File(filePath)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        return getModel(etextId, r);
    }
    
    // mostly for testing purposes
    public static Model getModel(final String etextId, final BufferedReader r) {
        final EtextStrInfo esi = getInfos(r);
        if (esi == null)
            return null;
        final List<Integer>[] tmpBreaks = TibetanStringChunker.getAllBreakingCharsIndexes(esi.totalString);
        final List<Integer>[] breaks = TibetanStringChunker.selectBreakingCharsIndexes(tmpBreaks, meanChunkPointsAim, maxChunkPointsAim, minChunkNbSylls);
        // tmpBreaks[3].get(0) is the total length in code points
        return getModel(breaks, tmpBreaks[3].get(0), esi.totalString, esi.breakList, etextId);
    }
    
    public static Model getModel(final List<Integer>[] breaks, final int totalStringPointNum, final String totalString, final List<Integer> initialBreakList, final String etextId) {
        final Model m = ModelFactory.createDefaultModel();
        final List<Integer> charBreaks = breaks[0];
        final List<Integer> pointBreaks = breaks[1];
        int chunkSeqNum = 1;
        int lastCharBreakIndex = 0;
        int lastPointBreakIndex = 0;
        final Resource etext = m.createResource(TransferHelpers.BDR+etextId);
        final Property hasChunk = m.getProperty(TransferHelpers.BDO, "eTextHasChunk");
        final Property seqNum = m.getProperty(TransferHelpers.BDO, "seqNum");
        final Property chunkContents = m.getProperty(TransferHelpers.BDO, "chunkContents");
        for (chunkSeqNum = 1 ; chunkSeqNum <= charBreaks.size() ; chunkSeqNum++) {// final int charBreakIndex : charBreaks) {
            final int charBreakIndex = charBreaks.get(chunkSeqNum-1);
            final int pointBreakIndex = pointBreaks.get(chunkSeqNum-1);
            final String contents = totalString.substring(lastCharBreakIndex, charBreakIndex);
            Resource chunk = m.createResource();
            etext.addProperty(hasChunk, chunk);
            chunk.addProperty(seqNum, m.createTypedLiteral(chunkSeqNum, XSDDatatype.XSDinteger));
            chunk.addProperty(chunkContents, m.createLiteral(contents, "bo")); // TODO: what about multilingual etexts?
            final int[] start = translatePoint(initialBreakList, lastPointBreakIndex+1, LOC_START);
            final int[] end = translatePoint(initialBreakList, pointBreakIndex, LOC_END);
            addChunkLocation(m, chunk, start[0], start[1], LOC_START);
            addChunkLocation(m, chunk, end[0], end[1], LOC_END);
            lastCharBreakIndex = charBreakIndex;
            lastPointBreakIndex = pointBreakIndex;
        }
        if (lastCharBreakIndex != totalString.length()) {
            final String contents = totalString.substring(lastCharBreakIndex);
            Resource chunk = m.createResource();
            etext.addProperty(hasChunk, chunk);
            chunk.addProperty(seqNum, m.createTypedLiteral(chunkSeqNum, XSDDatatype.XSDinteger));
            chunk.addProperty(chunkContents, m.createLiteral(contents, "bo"));
            final int[] start = translatePoint(initialBreakList, lastPointBreakIndex+1, LOC_START);
            final int[] end = translatePoint(initialBreakList, totalStringPointNum, LOC_END);
            addChunkLocation(m, chunk, start[0], start[1], LOC_START);
            addChunkLocation(m, chunk, end[0], end[1], LOC_END);
        }
        return m;
    }
}
