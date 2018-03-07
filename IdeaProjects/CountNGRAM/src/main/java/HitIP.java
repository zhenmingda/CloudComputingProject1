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

import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HitIP {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Configuration conf = context.getConfiguration();
            //String n=conf.get("Param");
            //int i=Integer.parseInt(n);
            StringTokenizer itr = new StringTokenizer(value.toString(),"\r\n");
            Pattern p = Pattern.compile("10\\.153\\.239\\.5");

            while (itr.hasMoreTokens()) {
                Matcher m = p.matcher(itr.nextToken());
                while (m.find()) {
                    word.set(m.group());
                    context.write(word, one);
                }
            }
            //String newvalue = value.toString().replace(" ", "");
            //Pattern p = Pattern.compile("“/assets/img/home-logo.png");
            //Matcher m = p.matcher(newvalue);
           /* Set<String> wordSet = new TreeSet<String>();
            while (newvalue.length() >= i) {
                while (m.find()) {
                    wordSet.add(m.group());
                }
                newvalue = newvalue.replaceFirst("\\w", "");

                m = p.matcher(newvalue);

            }
            for (String words : wordSet) {
                word.set(words);
                context.write(word, one);
            }*/

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
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String n = otherArgs[2];

        Job job = Job.getInstance(conf, "HitIP");

        job.setJarByClass(HitIP.class);
        job.setMapperClass(HitIP.TokenizerMapper.class);
        job.setCombinerClass(HitIP.IntSumReducer.class);
        job.setReducerClass(HitIP.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
