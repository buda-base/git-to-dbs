package io.bdrc.gittodbs;

import static io.bdrc.libraries.Models.getFacetNode;
import static io.bdrc.libraries.Models.FacetType.ETEXT_CHUNK;
import io.bdrc.gittodbs.TibetanStringChunker.BreaksInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtextContents {
    
    public static Logger logger = LoggerFactory.getLogger(EtextContents.class);

    public static int meanChunkPointsAim = 1200;
    public static int maxChunkPointsAim = 2400;
    public static int minChunkNbSylls = 6;
    
    private static ByteBuffer buf = ByteBuffer.allocate(32*1024*1024);

    public static Model getModel(final String filePath, final String etextId, Model etextM) {
        String content = "";
        buf.clear();

        try (RandomAccessFile etextFile = new RandomAccessFile(filePath, "r")) {        
            //            content = new String ( Files.readAllBytes( Paths.get(filePath) ), StandardCharsets.UTF_8 );
            FileChannel inChan = etextFile.getChannel();
            int bytesRead = inChan.read(buf);
            if (bytesRead > 0) {
                content = new String( buf.array(), 0, bytesRead, StandardCharsets.UTF_8 );
            } else {
                logger.error("getModel got " + bytesRead + " for " + filePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (etextM != null && !content.isEmpty()) {
            final BreaksInfo tmpBreaks = TibetanStringChunker.getAllBreakingCharsIndexes(content);
            final BreaksInfo breaks = TibetanStringChunker.selectBreakingCharsIndexes(tmpBreaks, meanChunkPointsAim, maxChunkPointsAim, minChunkNbSylls);
            return getModel(breaks, tmpBreaks.pointLen, content, etextId, etextM);
        } else {
            logger.error("getModel etextM == null: " + (etextM == null) + " with content.isEmpty(): " + content.isEmpty() + " for " + filePath);
            return null;
        }
    }

    private static Model getModel(final BreaksInfo breaks, final int totalStringPointNum, final String totalString, final String etextId,  Model etextM) {
        final Model m = etextM;
        final List<Integer> charBreaks = breaks.chars;
        final List<Integer> pointBreaks = breaks.points;
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
