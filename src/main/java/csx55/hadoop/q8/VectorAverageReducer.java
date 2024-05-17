package csx55.hadoop.q8;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class VectorAverageReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float[] sumVector = null;
        int count = 0;
        for (Text val : values) {
            String[] nums = val.toString().split("\\s+");
            if (sumVector == null) {
                sumVector = new float[nums.length];
            }
            for (int i = 0; i < nums.length; i++) {
                sumVector[i] += Float.parseFloat(nums[i]);
            }
            count++;
        }

        StringBuilder sb = new StringBuilder();
        for (float sum : sumVector) {
            sb.append(sum / count).append(" ");
        }

        Text newKey = new Text(key + "|");

        context.write(newKey, new Text(sb.toString().trim()));
    }
}


