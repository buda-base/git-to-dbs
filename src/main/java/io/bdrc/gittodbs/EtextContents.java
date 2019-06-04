package io.bdrc.gittodbs;

import static io.bdrc.libraries.Models.getFacetNode;
import static io.bdrc.libraries.Models.FacetType.ETEXT_CHUNK;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class EtextContents {

    public static int meanChunkPointsAim = 1200;
    public static int maxChunkPointsAim = 2400;
    public static int minChunkNbSylls = 6;
    
    public static class EtextStrInfo {
        public String totalString;
        public List<Integer> breakList;
        public EtextStrInfo(String totalString, List<Integer> breakList) {
            this.totalString = totalString;
            this.breakList = breakList;
        }
    }
    
    public static Model getModel(final String filePath, final String etextId, Model etextM) {
        String content = "";
        try {
            content = new String ( Files.readAllBytes( Paths.get(filePath) ) );
        } catch (IOException e) {
            e.printStackTrace();
        }
        final List<Integer>[] tmpBreaks = TibetanStringChunker.getAllBreakingCharsIndexes(content);
        final List<Integer>[] breaks = TibetanStringChunker.selectBreakingCharsIndexes(tmpBreaks, meanChunkPointsAim, maxChunkPointsAim, minChunkNbSylls);
        // tmpBreaks[3].get(0) is the total length in code points
        return getModel(breaks, tmpBreaks[3].get(0), content, etextId, etextM);
    }

    private static Model getModel(final List<Integer>[] breaks, final int totalStringPointNum, final String totalString, final String etextId,  Model etextM) {
        final Model m = etextM;
        final List<Integer> charBreaks = breaks[0];
        final List<Integer> pointBreaks = breaks[1];
        int chunkSeqNum = 1;
        int lastCharBreakIndex = 0;
        int lastPointBreakIndex = 0;
        final Resource etext = m.createResource(TransferHelpers.BDR+etextId);
        final Property hasChunk = m.getProperty(TransferHelpers.BDO, "eTextHasChunk");
        final Property chunkContents = m.getProperty(TransferHelpers.BDO, "chunkContents");
        for (chunkSeqNum = 1 ; chunkSeqNum <= charBreaks.size() ; chunkSeqNum++) {// final int charBreakIndex : charBreaks) {
            final int charBreakIndex = charBreaks.get(chunkSeqNum-1);
            final int pointBreakIndex = pointBreaks.get(chunkSeqNum-1);
            final String contents = totalString.substring(lastCharBreakIndex, charBreakIndex);
            Resource chunk = getFacetNode(ETEXT_CHUNK, etext);
            etext.addProperty(hasChunk, chunk);
            chunk.addProperty(chunkContents, m.createLiteral(contents, "bo")); // TODO: what about multilingual etexts?
            chunk.addProperty(m.getProperty(TransferHelpers.BDO, "sliceStartChar"), m.createTypedLiteral(lastPointBreakIndex, XSDDatatype.XSDinteger));
            chunk.addProperty(m.getProperty(TransferHelpers.BDO, "sliceEndChar"), m.createTypedLiteral(pointBreakIndex, XSDDatatype.XSDinteger));
            lastCharBreakIndex = charBreakIndex;
            lastPointBreakIndex = pointBreakIndex;
        }
        if (lastCharBreakIndex != totalString.length()) {
            final String contents = totalString.substring(lastCharBreakIndex);
            Resource chunk = getFacetNode(ETEXT_CHUNK, etext);
            etext.addProperty(hasChunk, chunk);
            chunk.addProperty(chunkContents, m.createLiteral(contents, "bo"));
            chunk.addProperty(m.getProperty(TransferHelpers.BDO, "sliceStartChar"), m.createTypedLiteral(lastPointBreakIndex, XSDDatatype.XSDinteger));
            chunk.addProperty(m.getProperty(TransferHelpers.BDO, "sliceEndChar"), m.createTypedLiteral(totalStringPointNum, XSDDatatype.XSDinteger));
        }
        return m;
    }
}
