import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HitMostIPSort {

    public static class SortMap extends Mapper<Text, Text, IntWritable, Text> {

        private IntWritable intSum = new IntWritable();

        public void map(Text path, Text sum, Context context) throws IOException, InterruptedException {
            int intVal = Integer.parseInt(sum.toString());
            intSum.set(intVal);
            context.write(intSum, path);
        }
    }

    public static class DescendingIntComparator extends WritableComparator {

        public DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "HitMostIPSort");
        job.setJarByClass(HitMostIP.class);

        job.setMapperClass(SortMap.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Sort with descending order
        job.setSortComparatorClass(DescendingIntComparator.class);

        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}