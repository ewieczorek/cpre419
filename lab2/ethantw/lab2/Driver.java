
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 2 *********************
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.lang.String;
import java.util.StringTokenizer;
import java.util.*;
import java.util.HashMap;
import java.lang.Object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/cpre419/gutenberg"; 
		String temp = "/user/ethantw/lab2/exp2/temp";
		String output = "/user/ethantw/lab2/exp2/output/"; 

		// The number of reduce tasks 
		int reduce_tasks = 4; 
		
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Driver Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class);

		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);

		
		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(IntWritable.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(IntWritable.class);
		job_one.setOutputValueClass(Text.class);

		// The class that provides the map method
		job_one.setMapperClass(Map_One.class);

		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));
		
		// This is legal
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path));
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// This is not allowed
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); 

		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1

		Job job_two = Job.getInstance(conf, "Driver Program Round Two");
		job_two.setJarByClass(Driver.class);
		job_two.setNumReduceTasks(1);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(IntWritable.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Run the job
		job_two.waitForCompletion(true);

		/**
		 * **************************************
		 * ************************************** 
		 * FILL IN CODE FOR MORE JOBS IF YOU NEED 
		 * **************************************
		 * **************************************
		 */

	}

	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text
	// (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can
	// be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		Text bigram = new Text();
		
		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();

			String charactersOnlyLine = line.replaceAll("[^A-Za-z0-9\\s]", "");
			String lowerCaseLine = charactersOnlyLine.toLowerCase();
			
			String previousWord = "";
			
			// Tokenize to get the individual words
			StringTokenizer tokens = new StringTokenizer(lowerCaseLine);
			while (tokens.hasMoreTokens()) {
				String currentWord = tokens.nextToken();
				
				if(!previousWord.equals("")) {
					bigram.set(previousWord + " " + currentWord);
					context.write(bigram, one);
				}
				previousWord = currentWord;
				//System.out.println(bigram.toString());
			} // End while

		} 
	} 

	
	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable> {

		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			
			for (IntWritable value : values) {

				int valueSum = value.get();
				sum += valueSum;
			}

			context.write(key, new IntWritable(sum));

		} 
	}

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {
		private static Map<String, Integer> bigramMap = new HashMap<String, Integer>();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitLine = line.split("\t");
			int count = Integer.valueOf(splitLine[1]);
			String bigram = splitLine[0];
				
			if(bigramMap.size() < 10) {
				bigramMap.put(bigram, count);
			}
			else {
				Map.Entry<String, Integer> minimumValue = null;
				for(Map.Entry<String, Integer> entry : bigramMap.entrySet()) {
					if(minimumValue == null) {
						minimumValue = entry;
					}
					if(entry.getValue() < minimumValue.getValue() ) {
						minimumValue = entry;
					}
				}
				if(count > minimumValue.getValue()) {
					bigramMap.remove(minimumValue.getKey());
					bigramMap.put(bigram, count);
				}
			}	
		} 
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Map.Entry<String, Integer> entry : bigramMap.entrySet()) {
	            IntWritable count = new IntWritable(entry.getValue());
	            Text value = new Text(entry.getKey() + "\t" + count.toString());
				context.write(new Text("key"), value);			
			}	
		}
	} 

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable> {
		//private static Map<String, Integer> bigramReduceMap = new HashMap<String, Integer>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> bigramReduceMap = new HashMap<String, Integer>();
			
            for (Text value : values) {
    			String line = value.toString();
    			String[] splitLine = line.split("\t");
    			int count = Integer.valueOf(splitLine[1]);
    			String bigram = splitLine[0];
    			
    			if(bigramReduceMap.size() < 10) {
    				bigramReduceMap.put(bigram, count);
    			}
    			else {
    				Map.Entry<String, Integer> minimumValue = null;
    				for(Map.Entry<String, Integer> entry : bigramReduceMap.entrySet()) {
    					if(minimumValue == null) {
    						minimumValue = entry;
    					}
    					if(entry.getValue() < minimumValue.getValue() ) {
    						minimumValue = entry;
    					}
    				}
    				if(count > minimumValue.getValue()) {
    					bigramReduceMap.remove(minimumValue.getKey());
    					bigramReduceMap.put(bigram, count);
    				}
    			}	
            }
			for(Map.Entry<String, Integer> entry : bigramReduceMap.entrySet()) {
	            IntWritable count = new IntWritable(entry.getValue());
	            Text value = new Text(entry.getKey());
				context.write(value, count);
			}	
		}
	} 

}
