package csx55.hadoop.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingIntWritableComparator extends WritableComparator {
    protected DescendingIntWritableComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        Integer v1 = readInt(b1, s1);
        Integer v2 = readInt(b2, s2);
        return v1.compareTo(v2) * (-1);  // Multiply by -1 to invert the order
    }
}

