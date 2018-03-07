import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.StringEscapeUtils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NgramCount {
	public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{
		
		ArrayList<ArrayList<String>> twoGram = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> threeGram = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> FourGram = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> FiveGram = new ArrayList<ArrayList<String>>();
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawText = value.toString();
			String cleanText="";
			try {
				cleanText = cleanedText(rawText);
			} catch (Exception e) {
				e.printStackTrace();
			} 
			StringTokenizer itr = new StringTokenizer(cleanText);
			while (itr.hasMoreTokens()) {
				String oneWord = itr.nextToken();
				word.set(oneWord);
				context.write(word, one);
				addToGram(twoGram, oneWord, 2, context);
				addToGram(threeGram, oneWord, 3, context);
				addToGram(FourGram, oneWord, 4, context);
				addToGram(FiveGram, oneWord, 5, context);
			}
		}
		
		public static void addToGram(ArrayList<ArrayList<String>> list, String word, int len, Context context) throws IOException, InterruptedException {
			boolean clear = false;
			for (int i = 0 ; i < list.size(); i++) {
				list.get(i).add(word);
				if (list.get(i).size() == len) {
					writeHelper(list.get(i), context);
					clear = true;
				}
			}
			if (clear) {
				list.remove(0);
				clear = false;
			}
			ArrayList<String> newList = new ArrayList<String>();
			newList.add(word);
			list.add(newList);	
		}
		
		public static void writeHelper(ArrayList<String> list, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			final IntWritable one = new IntWritable(1);
			StringBuilder bd = new StringBuilder();
			for (int i = 0; i < list.size() - 1; i++) {
				bd.append(list.get(i) + " ");
			}
			bd.append(list.get(list.size() - 1));
			word.set(bd.toString());
			context.write(word, one);
		}	
	 }

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			if (sum > 2) {
				result.set(sum);
				context.write(key, result);
			}
		  }
	}
	
	public static String cleanedText(String input) throws ParserConfigurationException, SAXException, IOException {
		// Parse XML
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		InputStream is = new ByteArrayInputStream(input.getBytes("UTF-8"));
		Document document = builder.parse(is);
		Element rootElement = document.getDocumentElement();
		NodeList list = rootElement.getElementsByTagName("revision");
		Element element = (Element) list.item(0);
		String text = StringEscapeUtils.unescapeHtml(element.getElementsByTagName("text").item(0).getTextContent());
		
		// Clean data
		text = text.toLowerCase();
		text = text.replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", " ");
		text = text.replaceAll("</ref>"," ");
		text = text.replaceAll("<ref>", " ");
		text = text.replaceAll("<ref class=\\w+>", " ");
		text = text.replaceAll("[^a-z']", " ");
		text = text.replaceAll("\'{2,}", " ");
		text = text.replaceAll("\'\\ +", " ");
		text = text.replaceAll("\\ +\'", " ").trim().replaceAll(" +", " ");
		return text;
	}

	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ngram count");
	    job.setJarByClass(NgramCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
