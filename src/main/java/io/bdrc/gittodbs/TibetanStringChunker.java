package io.bdrc.gittodbs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TibetanStringChunker {

    public static boolean fix0F7F = true;
    public static int maxQuatrainNbSylls = 15;
    public static int minQuatrainNbSylls = 7;
    public static int maxSmallGroupSize = 8; // multiple of 4 to avoid side effect with quatrain grouping
    public static boolean smallsGroupedWithPreceding = false; // group trail of small chunks with preceding chunk
    
    public static enum CharType {
        HEAD, // characters always at the front of a chunk
        SOFTHEAD, // same for emergency cases
        TAIL, // characters always at the end of a chunk
        SOFTTAIL, // same for emergency cases
        CHAR, // normal characters
        SHAD, // shad
        COMPLEX, // characters that are at the front of a chunk when surrounded by space, or at the end otherwise
        SHAD_LIKE_LETTER,
        VOWEL,
        SPACE,
        SYLLABLE_DELIMITER
        ;  
      }
    
    public static CharType getCharType(int codePoint) {
        switch (codePoint) {
        case 0x0fd0:
        case 0x0fd1:
        case 0x0fd3:
        case 0x0fd4:
        case 0x0f36:
        case 0x0f38:
        case 0x0f12:
        case 0x0f13:
        case 0x0f3a:
        case 0x0f01:
        case 0x0f02:
        case 0x0f03:
        case 0x0f04:
        case 0x0f05:
        case 0x0f06:
        case 0x0f07:
        case 0x0f08:
        case 0x0f09:
        case 0x0f0a:
            return CharType.HEAD;
        case 0x0f0f:
        case 0x0f10:
        case 0x0f11:
            return CharType.COMPLEX;
        case 0x0f3f:
        case 0x0f3c:
        case 0x0fd9:
            return CharType.SOFTHEAD;
        case 0x0f14:
        case 0x0f34: // ?
        case 0x0f3b:
            return CharType.TAIL;
        case 0x0f0b:
        case 0x0f0c:
        case 0x0fd2:
        case 0x0f7f:
            return CharType.SYLLABLE_DELIMITER;
        case 0x0fda:
        case 0x0f3e:
        case 0x0f3d:
            return CharType.SOFTTAIL;
        case 0x0f0d:
        case 0x0f0e:
            return CharType.SHAD;
        case 0x0f40:
        case 0x0f42:
        case 0x0f64:
            return CharType.SHAD_LIKE_LETTER;
        // vowels of ka, ga and sha after which no shad is added 
        case 0x0f72:
        case 0x0f7a:
        case 0x0f7c:
        case 0x0f7e:
        case 0x0f80:
        case 0x0f82:
        case 0x0f83:
            return CharType.VOWEL;
        case 0x0020:
            return CharType.SPACE;
        }
        // warning: in some texts '\u0f7f' is erroneously used for '\u0f14', in this case it's a tail
        return CharType.CHAR;
    }
    
    public static final int MODE_CHAR = 0;
    public static final int MODE_TAIL = 1;
    public static final int MODE_HEAD = 2;
    public static final int MODE_AFTER_SHAD_LIKE = 3;
    
    public static List<Integer>[] getBreakingIndexes(final String totalStr, final int meanChunkPointsAim, final int maxChunkPointsAim, final int minChunkNbSylls) {
        final List<Integer>[] allIndexes = getAllBreakingCharsIndexes(totalStr);
        final List<Integer>[] res = selectBreakingCharsIndexes(allIndexes, meanChunkPointsAim, maxChunkPointsAim, minChunkNbSylls);
        return res;
    }
    
    public static List<Integer>[] getAllBreakingCharsIndexes(final String totalStr) {
        final List<Integer> resChars = new ArrayList<Integer>();
        final List<Integer> resPoints = new ArrayList<Integer>();
        final List<Integer> resNbSylls = new ArrayList<Integer>();
        int curCharIndex = 0;
        int curPointIndex = 0;
        int curNbSylls = 0;
        int previousPoint = -1;
        final int len = totalStr.length();
        int curMode = MODE_HEAD;
        boolean lastIsDelimiter = false;
        while (curCharIndex < len) {
            // only break on spaces or \n:
            final int curPoint = totalStr.codePointAt(curCharIndex);
            boolean doBreak = false;
            CharType ct = getCharType(curPoint);
            if (ct == CharType.COMPLEX) {
                // if surrounded by space, then HEAD, else TAIL
                if ((previousPoint == -1 || previousPoint == 0x0020) && curCharIndex < len && totalStr.codePointAt(curCharIndex+1) == 0x0020) {
                    ct = CharType.HEAD;
                } else {
                    ct = CharType.TAIL;
                }
            }
            // case where 0F7F is used instead of 0F14, with a space afterwards
            if (curPoint == 0x0F7F && fix0F7F && curCharIndex < len-1 && totalStr.codePointAt(curCharIndex+1) == 0x0020) {
                ct = CharType.TAIL;
            }
            switch (curMode) {
            case MODE_TAIL:
                switch(ct) {
                // if we're in a tail, we break for non-tail things:
                case HEAD:
                case SOFTHEAD:
                    doBreak = true;
                    curMode = MODE_HEAD;
                    break;
                case CHAR:
                case VOWEL:
                    doBreak = true;
                    curMode = MODE_CHAR;
                    break;
                case SHAD_LIKE_LETTER:
                    doBreak = true;
                    curMode = MODE_AFTER_SHAD_LIKE;
                    break;
                default:
                    break;
                }
                break;
            case MODE_HEAD:
                // if we're in a head, we switch to char mode when encountering a char
                switch (ct) {
                case CHAR:
                case VOWEL:
                    curMode = MODE_CHAR;
                    curNbSylls = 1;
                    break;
                case SHAD_LIKE_LETTER:
                    curMode = MODE_AFTER_SHAD_LIKE;
                    curNbSylls = 1;
                    break;
                default:
                    break;
                }
                break;
            case MODE_CHAR:
                // if in normal mode:
                // - we go to tail mode when encountering a tail or a shad
                // - we break and go to head mode when encountering a head
                switch (ct) {
                case SHAD_LIKE_LETTER:
                    curMode = MODE_AFTER_SHAD_LIKE;
                    // no break
                case CHAR:
                case VOWEL:
                    if (lastIsDelimiter)
                        curNbSylls += 1;
                    break;
                case SHAD:
                case TAIL:
                    curMode = MODE_TAIL;
                    break;
                case HEAD:
                    curMode = MODE_HEAD;
                    doBreak = true;
                    break;
                default:
                    break;
                }
                break;
            case MODE_AFTER_SHAD_LIKE:
                // same same but different:
                // - we switch back to char mode if a non-vowel is encountered
                // - space acts like a shad
                switch (ct) {
                case CHAR:
                case SOFTHEAD:
                case SOFTTAIL:
                case SYLLABLE_DELIMITER:
                    curMode = MODE_CHAR;
                    // no break
                case VOWEL:
                case SHAD_LIKE_LETTER:
                    if (lastIsDelimiter)
                        curNbSylls += 1;
                    break;
                case SPACE:
                case SHAD:
                case TAIL:
                    curMode = MODE_TAIL;
                    break;
                case HEAD:
                    curMode = MODE_HEAD;
                    doBreak = true;
                    break;
                default:
                    break;
                }
                break;
            }
            if (doBreak) {
                resChars.add(curCharIndex);
                resPoints.add(curPointIndex);
                resNbSylls.add(curNbSylls);
                if (ct == CharType.SHAD_LIKE_LETTER || ct == CharType.VOWEL || ct == CharType.CHAR)
                    curNbSylls = 1;
                else
                    curNbSylls = 0;
            }
            curCharIndex += Character.charCount(curPoint);
            curPointIndex += 1;
            previousPoint = curPoint;
            lastIsDelimiter = (ct == CharType.SYLLABLE_DELIMITER); 
        }
        resNbSylls.add(curNbSylls); // adding the final chunk's number of syllables 
        List<Integer>[] res = new List[4];
        res[0] = resChars;
        res[1] = resPoints;
        res[2] = resNbSylls;
        res[3] = Arrays.asList(curPointIndex); // to get the string length in terms of points
        return res;
    }
    
    public static List<Integer>[] selectBreakingCharsIndexes(final List<Integer>[] allIndexes, final int meanChunkPointsAim, final int maxChunkPointsAim, final int minChunkNbSylls) {
        final List<Integer> resChars = new ArrayList<Integer>();
        final List<Integer> resPoints = new ArrayList<Integer>();
        
        Map<Integer,Boolean> breakIndexes = new HashMap<Integer,Boolean>();
        for (int i = 0; i < allIndexes[0].size(); i++) {
            breakIndexes.put(i, true);
        }

        filterQuatrains(breakIndexes, allIndexes);
        filterSmalls(breakIndexes, allIndexes, minChunkNbSylls);
        filterRegroup(breakIndexes, allIndexes, meanChunkPointsAim, maxChunkPointsAim);

        for (int i = 0; i < allIndexes[0].size(); i++) {
            if (breakIndexes.get(i)) {
                resChars.add(allIndexes[0].get(i));
                resPoints.add(allIndexes[1].get(i));
            }
        }
        
        List<Integer>[] res = new List[2];
        res[0] = resChars;
        res[1] = resPoints;
        return res;
    }

    public static void filterRegroup(final Map<Integer, Boolean> breakIndexes, final List<Integer>[] allIndexes,
            final int meanChunkPointsAim, final int maxChunkPointsAim) {
        int lastBreakPointIndex = -1;
        int curIndex = 0;
        int lastPossibleBreakPointIndex = -1;
        int lastPossibleBreakIndex = -1;
        final int totalpoints = allIndexes[3].get(0);
        // we add the total number of points in string at the end of allIndexes[1] in a temporary way:
        allIndexes[1].add(totalpoints);
        final int finalIndex = allIndexes[1].size()-1;
        for (Integer nbPoints : allIndexes[1]) {
            final int totalPointsBeforeThis = nbPoints - lastBreakPointIndex;
            if (totalPointsBeforeThis < meanChunkPointsAim) {
                if (curIndex == finalIndex)
                    continue;
                if (breakIndexes.get(curIndex)) {
                    // we fill it with false, but the last one may be set to true afterwards
                    // each time we set breakIndexes[curIndex] we make sure we don't add a spurious
                    // entry due to the additional entry at the end
                    breakIndexes.put(curIndex, false);
                    lastPossibleBreakPointIndex = nbPoints;
                    lastPossibleBreakIndex = curIndex;
                }
            } else if (totalPointsBeforeThis >  maxChunkPointsAim) {
                if (nbPoints - lastPossibleBreakPointIndex < maxChunkPointsAim) {
                    breakIndexes.put(lastPossibleBreakIndex, true);
                    if (curIndex != finalIndex)
                        breakIndexes.put(curIndex, false);
                    lastBreakPointIndex = lastPossibleBreakPointIndex;
                } else if (curIndex != 0) {
                    // we force a break even if there should not be one:
                    breakIndexes.put(curIndex -1, true);
                    if (curIndex != finalIndex)
                        breakIndexes.put(curIndex, false);
                    lastBreakPointIndex = curIndex -1;
                }
                lastPossibleBreakIndex = -1;
                lastPossibleBreakPointIndex = -1;
            } else {
                if (lastPossibleBreakIndex == -1) {
                    lastBreakPointIndex = nbPoints;
                } else {
                    final int totalPointsBeforeLast = lastPossibleBreakPointIndex - lastBreakPointIndex;
                    if (meanChunkPointsAim - totalPointsBeforeLast < totalPointsBeforeThis - meanChunkPointsAim) {
                        // mean chunk size is closer to the break before:
                        breakIndexes.put(lastPossibleBreakIndex, true);
                        if (curIndex != finalIndex)
                            breakIndexes.put(curIndex, false);
                        lastBreakPointIndex = lastPossibleBreakPointIndex;
                    } else {
                        // this index is already set to true
                        lastBreakPointIndex = nbPoints;
                    }
                    lastPossibleBreakIndex = -1;
                    lastPossibleBreakPointIndex = -1;
                }
            }
            curIndex += 1;
        }
        // we remove the final index we added at the beginning:
        allIndexes[1].remove(finalIndex);
    }

    public static void filterSmalls(Map<Integer, Boolean> breakIndexes, List<Integer>[] allIndexes, int minChunkNbSylls) {
        int curIndex = 0;
        int nbInCurGroup = 0;
        for (Integer nbSyllables : allIndexes[2]) {
            if (nbSyllables < minChunkNbSylls) {
                if (nbInCurGroup >= maxSmallGroupSize) {
                    nbInCurGroup = 1;
                } else {
                    if (curIndex > 0 && (smallsGroupedWithPreceding || nbInCurGroup > 0))
                        breakIndexes.put(curIndex-1, false);
                    nbInCurGroup += 1;
                }
            } else {
                nbInCurGroup = 0;
            }
            curIndex += 1;
        }
    }

    public static void filterQuatrains(Map<Integer, Boolean> breakIndexes, List<Integer>[] allIndexes) {
        int curIndex = 0;
        int lineOfQuatrain = 0;
        int quatrainNbSylls = 0;
        for (Integer nbSyllables : allIndexes[2]) {
            if (lineOfQuatrain == 0) {
                if (nbSyllables <= maxQuatrainNbSylls && nbSyllables >= minQuatrainNbSylls) {
                    lineOfQuatrain = 1;
                    quatrainNbSylls = nbSyllables;
                }
            } else {
                if (quatrainNbSylls == nbSyllables) {
                    lineOfQuatrain += 1;
                    if (lineOfQuatrain == 4) {
                        lineOfQuatrain = 0;
                        quatrainNbSylls = 0;
                        breakIndexes.put(curIndex-1, false);
                        breakIndexes.put(curIndex-2, false);
                        breakIndexes.put(curIndex-3, false);
                    }
                } else {
                    if (nbSyllables <= maxQuatrainNbSylls && nbSyllables >= minQuatrainNbSylls) {
                        lineOfQuatrain = 1;
                        quatrainNbSylls = nbSyllables;
                    } else {
                        lineOfQuatrain = 0;
                        quatrainNbSylls = 0;
                    }
                }
            }
            curIndex += 1;
        }
    }
}
