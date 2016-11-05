package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.io.Text;

import java.util.HashMap;

public class Matches {
    public HashMap<Text, StringToIntMapWritable> map;

    public Matches() {
        map = new HashMap<>();
    }

    public void addMatch(Text word1, Text word2) {
        addMatch(word1, word2, 1);
    }

    public void addMatch(Text word1, Text word2, long count) {
        if (!word1.equals(word2)) {
            StringToIntMapWritable wordMatches = map.get(word1);
            if (wordMatches == null) {
                wordMatches = new StringToIntMapWritable();
                map.put(word1, wordMatches);
            }
            wordMatches.addMatch(word2, count);
        }
    }

}

