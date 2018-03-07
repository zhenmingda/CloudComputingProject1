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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HitMostPath {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Configuration conf = context.getConfiguration();
            //String n=conf.get("Param");
            //int i=Integer.parseInt(n);
            StringTokenizer itr1 = new StringTokenizer(value.toString(),"\r\n");


            //StringTokenizer itr = new StringTokenizer(value.toString(),"\r\n");
            Pattern p = Pattern.compile("(http\\:\\/\\/www\\.the\\-associates\\.co\\.uk)?([^\\?\\&\\,]+)");

            while (itr1.hasMoreTokens()) {
                String itr[]=itr1.nextToken().split(" ");

                Matcher m = p.matcher(itr[6]);
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

            //int i = compare(sum);

            //ArrayList<TreeMap> list =new ArrayList<>();


            //list.add(map);
            result.set(sum);
            context.write(key, result);
        }


    }





    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String n = otherArgs[2];

        Job job = Job.getInstance(conf, "HitMostPath");

        job.setJarByClass(HitMostPath.class);
        job.setMapperClass(HitMostPath.TokenizerMapper.class);
        job.setCombinerClass(HitMostPath.IntSumReducer.class);
        job.setReducerClass(HitMostPath.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
