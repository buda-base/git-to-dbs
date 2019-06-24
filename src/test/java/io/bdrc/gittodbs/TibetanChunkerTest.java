package io.bdrc.gittodbs;

import io.bdrc.gittodbs.TibetanStringChunker.BreaksInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.*;
import io.bdrc.gittodbs.TibetanStringChunker;

public class TibetanChunkerTest {

    @Test
    public void testAllIndexes() {
        BreaksInfo res = TibetanStringChunker.getAllBreakingCharsIndexes("x། xx");
        assertThat(res.chars, contains(3));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("x། །xx");
        assertThat(res.chars, contains(4));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("x༑ x");
        assertThat(res.chars, contains(3));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("x ༑ x");
        assertThat(res.chars, contains(2));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("༑ x");
        assertTrue(res.chars.isEmpty());
        res = TibetanStringChunker.getAllBreakingCharsIndexes("x ༑x");
        assertThat(res.chars, contains(3));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("x\u0f14x");
        assertThat(res.chars, contains(2));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("x\u0f7f x");
        assertThat(res.chars, contains(3));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("xxx།། །། ༆ ། །xxx");
        assertThat(res.chars, contains(9));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("xxx༎ ༎༆ ༎xxx");
        assertThat(res.chars, contains(6));
        res = TibetanStringChunker.getAllBreakingCharsIndexes("སྤྱི་ལོ་༢༠༡༧ ཟླ་༡ ཚེས་༡༤ ཉིན་ལ་བྲིས་པ་དགེ");
        assertTrue(res.chars.isEmpty());
        res = TibetanStringChunker.getAllBreakingCharsIndexes("བཀྲིས་ ༼བཀྲ་ཤིས༽ ངའི་གྲོགས་པོ་རེད།");
        assertTrue(res.chars.isEmpty());
        res = TibetanStringChunker.getAllBreakingCharsIndexes("ག གི གྲ ཀ ཤ པ མ");
        assertThat(res.chars, contains(2, 5, 10, 12));
    }
    
    @Test
    public void testNbSylls() {
        BreaksInfo res = TibetanStringChunker.getAllBreakingCharsIndexes("༄༅། ཀ༌ཀོ་ཀཿཀ࿒ཀ་ཀ ཀ་རང་ཀ།་");
        assertThat(res.nbSylls, contains(6, 3));
    }
    
    public static Map<Integer,Boolean> getNewBreakIndexes(int size, Boolean value) {
        Map<Integer,Boolean> breakIndexes = new HashMap<Integer,Boolean>();
        for (int i = 0; i < size; i++) {
            breakIndexes.put(i, value);
        }
        return breakIndexes;
    }
    
    @Test
    public void testSelection() {
        // test quatrain grouping
        BreaksInfo allIndexes = new BreaksInfo();
        allIndexes.nbSylls = Arrays.asList(7,7,7,7, 1, 9,9,9,9, 1, 7,7,7, 6, 7,7,7,7, 8,8,8,8,8, 7,7,7,7);
        Map<Integer,Boolean> breakIndexes = getNewBreakIndexes(allIndexes.nbSylls.size()-1, true);
        TibetanStringChunker.filterQuatrains(breakIndexes, allIndexes);
        assertThat(breakIndexes.values(), contains(false, false, false, true, true, false, false, false, true, true, true, true, true, true, false, false, false, true, false, false, false, true, true, false, false, false));
        // test small grouping
        allIndexes.nbSylls = Arrays.asList(2,2, 3, 2,1,2, 4, 4, 2,2,2,2,2,2,2,2, 2,2,2);
        breakIndexes = getNewBreakIndexes(allIndexes.nbSylls.size()-1, true);
        TibetanStringChunker.filterSmalls(breakIndexes, allIndexes, 3);
        assertThat(breakIndexes.values(), contains(false, true, true, false, false, true, true, true, false, false, false, false, false, false, false, true, false, false));
        // test chunk grouping to build chunks of approximate equal size, no max
        // we need a mutable list for the test:
        allIndexes.points = new ArrayList<>(Arrays.asList(49, 100, 151, 200, 225, 275, 325, 349, 399, 440));
        allIndexes.pointLen = 490;
        breakIndexes = getNewBreakIndexes(allIndexes.points.size(), true);
        TibetanStringChunker.filterRegroup(breakIndexes, allIndexes, 50, 5000);
        assertThat(breakIndexes.values(), contains(true, true, true, true, false, true, true, false, true, true));
        // test with max length value
        breakIndexes = getNewBreakIndexes(allIndexes.points.size(), false);
        TibetanStringChunker.filterRegroup(breakIndexes, allIndexes, 49, 50);
        assertThat(breakIndexes.values(), contains(true, true, true, true, true, true, true, true, true, true));
    }
}
