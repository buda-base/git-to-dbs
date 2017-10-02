package io.bdrc.gittodbs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class EtextMerger {

    public static class EtextStrInfo {
        public String totalString;
        public List<Integer> breakList;
        public EtextStrInfo(String totalString, List<Integer> breakList) {
            this.totalString = totalString;
            this.breakList = breakList;
        }
    }
    
    public static EtextStrInfo getInfos(String origString) {
        List<Integer> breakList = new ArrayList<Integer>();
        StringBuilder sb = new StringBuilder();
        int lastCharIndex = 0;
        int currentTransformedPointIndex = 0;
        for (int charIndex = origString.indexOf('\n');
                charIndex >= 0;
                charIndex = origString.indexOf('\n', charIndex + 1))
           {
               final String line = origString.substring(lastCharIndex, charIndex);
               lastCharIndex = charIndex+1;
               if (line.isEmpty())
                   continue;
               final int lineLenPoints = line.codePointCount(0, line.length());
               sb.append(line);
               currentTransformedPointIndex += lineLenPoints;
               breakList.add(currentTransformedPointIndex);
               
           }
        final int origCharLength = origString.length();
        sb.append(origString.substring(lastCharIndex, origCharLength));
        // case of a final \n
        if (lastCharIndex == origCharLength) {
            breakList.remove(breakList.size()-1);
        }
        return new EtextStrInfo(sb.toString(), breakList);
    }
    
    public static EtextStrInfo getInfos(BufferedReader r) {
        List<Integer> breakList = new ArrayList<Integer>();
        StringBuilder sb = new StringBuilder();
        int currentTransformedPointIndex = 0;
        try {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                if (line.isEmpty())
                    continue;
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
}
