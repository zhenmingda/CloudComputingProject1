import java.io.IOException;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.file.SyncableFileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CountGram {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String n=conf.get("Param");
            int i=Integer.parseInt(n);
            StringTokenizer itr = new StringTokenizer(value.toString()," ");
            //String newvalue = value.toString().replace(" ", "");
            Pattern p = Pattern.compile(".{"+i+"}");
            while(itr.hasMoreTokens()) {
                String newvalue = itr.nextToken();

                Matcher m = p.matcher(newvalue);
                Set<String> wordSet = new TreeSet<String>();
                while (newvalue.length() >= i) {
                    while (m.find()) {
                        wordSet.add(m.group());
                    }
                    newvalue = newvalue.replaceFirst(".", "");

                    m = p.matcher(newvalue);

                }


                for (String words : wordSet) {
                    word.set(words);
                    context.write(word, one);
                }
            }
            // while (itr.hasMoreTokens()) {
            // System.out.println(itr.nextToken());
            //word.set();
            //context.write(word, one);
            //}
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String n = otherArgs[2];
        conf.set("Param",n);
        Job job = Job.getInstance(conf, "Count N Grams");

        job.setJarByClass(CountGram.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
