package csx55.hadoop.common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import java.nio.ByteBuffer;

public class DescendingDoubleWritableComparator extends WritableComparator {
    public DescendingDoubleWritableComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
        Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();
        return v1.compareTo(v2) * (-1);  // Multiply by -1 to invert the order
    }
}
