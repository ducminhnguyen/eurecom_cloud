package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class StringToIntMapWritable implements Writable {

    public HashMap<Text, Long> map;

    public StringToIntMapWritable() {
        map = new HashMap<>();
    }

    public void addMatch(Text word, long count) {
        Long cnt = map.get(word);
        if (cnt == null) {
            cnt = 0L;
            map.put(word, cnt);
        }
        map.put(word, cnt + count);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LongWritable longWritable = new LongWritable();
        longWritable.set(map.size());
        longWritable.write(out);
        for (Text word : map.keySet()) {
            word.write(out);
            longWritable.set(map.get(word));
            longWritable.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        map.clear();
        LongWritable longWritable = new LongWritable();
        longWritable.readFields(in);
        long numWord = longWritable.get();
        for (int i = 0; i < numWord; i++) {
            Text text = new Text();
            text.readFields(in);
            longWritable.readFields(in);
            map.put(text, longWritable.get());
        }
    }
}