package com.mercury.kaggle.mapreduce;

import com.mercury.kaggle.constants.ColumnNameConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CategoryCounting extends Configured implements Tool {

    // private static final Logger LOG = LoggerFactory.getLogger(CategoryCounting.class);
    
    private static final String SEPARATOR = ":";


    public static class CountingMapper extends TableMapper<Text, IntWritable> {
        

        private final IntWritable label = new IntWritable();
        private Text text = new Text();


        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // Only for one category here TODO for 23 ones
            byte[] cell = value.getValue(ColumnNameConstants.DATA_COLUMN_FAMILY, ColumnNameConstants.C2);
            byte[] cellLabel = value.getValue(ColumnNameConstants.DATA_COLUMN_FAMILY, ColumnNameConstants.LABEL);
            if (cell != null && cellLabel != null) {
                // Only category value,TODO counting on bit mask
                String key = "c2" + SEPARATOR + Bytes.toInt(cell);
                text.set(key);
                label.set(Bytes.toBoolean(cellLabel) ? 1 : 0);
                context.write(text, label);
            }
        }
    }


    public static class CountingCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class CountingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text text = new Text();

        private IntWritable counter = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int size = 0;
            for (IntWritable val : values) {
                count += val.get();
                size++;
            }
            text.set(new String(key.getBytes()) + SEPARATOR + "label");
            counter.set(count);
            context.write(text, counter);
            text.set(new String(key.getBytes()) + SEPARATOR + "counting");
            counter.set(size);
            context.write(text, counter);
        }
    }


    public int run(String[] args) throws Exception {
        // Hbase config
        Configuration config = HBaseConfiguration.create();
        config.addResource(this.getClass().getClassLoader().getResource("hbase-site.xml"));
        // Job initialization
        Job job = Job.getInstance(config, "TrackingEventCounting");
        job.setJarByClass(CategoryCounting.class);
        TableMapReduceUtil.initTableMapperJob("kaggle-train", new Scan(), CountingMapper.class, Text.class, IntWritable.class, job);
        // job.setCombinerClass(CountingCombiner.class);
        job.setReducerClass(CountingReducer.class);
        String path = "/tmp/stats_" + System.currentTimeMillis() + ".txt";
        FileOutputFormat.setOutputPath(job, new Path(path));
        // Job submission
        boolean b = job.waitForCompletion(true);
        if (!b) {
            return 0;
        }
        return 1;
    }




    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CategoryCounting(), args);
        System.exit(res);
    }

}
