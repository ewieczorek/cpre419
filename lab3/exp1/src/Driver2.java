
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

public class Driver2 {

	private static int triangles = 0;
	private static int triplets = 0;

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/cpre419/patents.txt"; 
		String temp = "/user/ethantw/lab3/exp2/temp";
		String temp2 = "/user/ethantw/lab3/exp2/temp2";
		String output = "/user/ethantw/lab3/exp2/output/";


		// The number of reduce tasks 
		int reduce_tasks = 8; 
		
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Driver Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Driver2.class);

		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);

		
		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(IntWritable.class);
		job_one.setMapOutputValueClass(IntWritable.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(IntWritable.class);
		job_one.setOutputValueClass(IntWritable.class);

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
		job_two.setJarByClass(Driver2.class);
		job_two.setNumReduceTasks(4);

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
		FileOutputFormat.setOutputPath(job_two, new Path(temp2));

		// Run the job
		job_two.waitForCompletion(true);

		
		//job three
		Job job_three = Job.getInstance(conf, "Driver Program Round Three");
		job_three.setJarByClass(Driver2.class);
		job_three.setNumReduceTasks(1);

		// Should be match with the output datatype of mapper and reducer
		job_three.setMapOutputKeyClass(Text.class);
		job_three.setMapOutputValueClass(Text.class);
		job_three.setOutputKeyClass(Text.class);
		job_three.setOutputValueClass(Text.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_three.setMapperClass(Map_Three.class);
		job_three.setReducerClass(Reduce_Three.class);

		job_three.setInputFormatClass(TextInputFormat.class);
		job_three.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_three, new Path(temp2));
		FileOutputFormat.setOutputPath(job_three, new Path(output));

		// Run the job
		job_three.waitForCompletion(true);
		
	}

	
	public static class Map_One extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer line = new StringTokenizer(value.toString());

			IntWritable node1 = new IntWritable(Integer.parseInt(line.nextToken()));
			IntWritable node2 = new IntWritable(Integer.parseInt(line.nextToken()));

			//pair these up
			context.write(node1, node2);
			context.write(node2, node1);
		} 
	}

	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
		Text text1 = new Text();
		Text text2 = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> inputSet = new HashSet<String>();
			Set<String> outputSet = new HashSet<String>();
			for (Text value : values) {
				String[] lineArray = value.toString().split(" ");
				if(lineArray[1].equals("input")) {
					inputSet.add(lineArray[0]);
				}
				if(lineArray[1].equals("output")) {
					outputSet.add(lineArray[0]);
				}
			}
			for(String strin : inputSet) {
				text1.set(strin);
				context.write(text1, key);
				context.write(key, text1);
			}
			Integer[] arr = list.toArray(new Integer[list.size()]);
			Arrays.sort(arr);

			Integer[] writeArr = new Integer[3];


			for(int i = 0; i < arr.length - 1; i++){
				for(int j = i + 1; j < arr.length; j++){
					writeArr[0] = arr[i];
					writeArr[1] = arr[j];
					writeArr[2] = tempKey;
					Arrays.sort(writeArr);

					context.write(new Text(writeArr[0].toString() + "-" + writeArr[1].toString() + "-" + writeArr[2].toString()), one);
				}
			}
		}
	}
	
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer line = new StringTokenizer(value.toString());
			Text text1 = new Text(line.nextToken());
			Text text2 = new Text(line.nextToken());
			context.write(text1, text2);
		} 
	} 

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, Text> {
		Set<String> doubleEdges = new HashSet<String>();
		Text text1 = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Set<String> valueSet = new HashSet<String>();
			int sumTriplets = 0;
			int sumTriangles = 0;
			for (Text value : values) {
				String[] splitter = value.toString().split(" ");
				if(splitter.length == 1) {
					doubleEdges.add(splitter[0]);
				} else {
					sumTriplets += 1;
					if(doubleEdges.contains(splitter[1])) {
						sumTriangles += 1;
					}
				}
            }
			text1.set(sumTriplets + " " + sumTriangles);
			context.write(key, text1);
		}
	} 

	
	// The third Map Class
	public static class Map_Three extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text("key"), value);
		} 
	} 

	// The second Reduce class
	public static class Reduce_Three extends Reducer<Text, Text, Text, Text> {
		private static Map<String, Integer> valueReduceMap = new HashMap<String, Integer>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sumTriplets = 0;
			double sumTriangles = 0;
			
			for (Text value : values) {
				String line = value.toString();
				String[] splitLine = line.split("\\t");
				String[] splitSums = splitLine[1].split(" ");
				sumTriplets += Double.parseDouble(splitSums[0]);
				sumTriangles += Double.parseDouble(splitSums[1]);
            }
			double GCC = sumTriangles/(sumTriplets);
			context.write(new Text("GCC"), new Text(String.valueOf(GCC)));
		}
	} 
}
