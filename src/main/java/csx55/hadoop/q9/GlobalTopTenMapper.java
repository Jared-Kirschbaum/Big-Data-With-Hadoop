package csx55.hadoop.q9;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.log4j.Logger;

public class GlobalTopTenMapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable frequency = new IntWritable();

    private static final Logger LOG = Logger.getLogger(GlobalTopTenMapper.class);


    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] termAndCount = value.toString().split("\\s+");
        LOG.info(value);
        LOG.info(termAndCount[0]);
        LOG.info(termAndCount[1]);
        int count = Integer.parseInt(termAndCount[1]);
        frequency.set(count);
        context.write(frequency, new Text(termAndCount[0]));
    }
}

