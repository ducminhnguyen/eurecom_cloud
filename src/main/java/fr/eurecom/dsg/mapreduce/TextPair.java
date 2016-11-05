package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API)
 * and Comparable to itself.
 */
public class TextPair implements WritableComparable<TextPair> {

    // add the pair objects as TextPair fields
    public Text first;
    public Text second;

    public TextPair() {
        // implement the constructor, empty constructor MUST be implemented for deserialization
    }

    public TextPair(String first, String second) {
        this.first = new Text(first);
        this.second = new Text(second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write to out the serialized version of this such that can be deserializated in future. This will be use to write to HDFS
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // read from in the serialized version of a Pair and deserialize it
        first = new Text();
        first.readFields(in);
        second = new Text();
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        // implement hash (inspired from android.utils.Pair)
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        // implement equals
        if (o instanceof TextPair) {
            TextPair textPair = (TextPair) o;
            return textPair.first.equals(first) && textPair.second.equals(second);
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(TextPair tp) {
        // implement the comparison between this and tp
        int res = first.compareTo(tp.first);
        if (res == 0) {
            res = second.compareTo(tp.second);
        }
        return res;
    }

    @Override
    public String toString() {
        // implement toString for text output format
        return "(" + first + ", " + second + ")";
    }


// DO NOT TOUCH THE CODE BELOW

    /**
     * Compare two pairs based on their values
     */
    public static class Comparator extends WritableComparator {

        /**
         * Reference to standard Hadoop Text comparator
         */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }

    /**
     * Compare just the first element of the Pair
     */
    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }

    }
}