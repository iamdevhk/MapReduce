import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;


public class InvertedIndexing {
	
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		JobConf jobConfObj;
		
		public void configure(JobConf job) {
			this.jobConfObj = job;
		}
		
		// This is a Map function in the context of Hadoop MapReduce
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws
				IOException {
			
			// Retrieve the number of keywords from the JobConf
			int argc = Integer.parseInt(jobConfObj.get("argc"));
			 // Create a Set to store the keywords
			Set<String> args = new HashSet();
			// Retrieve the keywords from the JobConf and add them to the Set
			for (int itr = 0; itr < argc; itr++) {
				args.add(jobConfObj.get("keyword" + itr));
			}
			// Get the name of the current file being processed
			FileSplit fileSplitVal = (FileSplit) reporter.getInputSplit();
			String curFileName = "" + fileSplitVal.getPath().getName();
			String lines = value.toString();
			// Tokenize the input String into individual words
			StringTokenizer strTokenizer = new StringTokenizer(lines);
			// Iterate through the tokens and check if they match any of the keywords
			while (strTokenizer.hasMoreTokens()) {
				String nxtToken = strTokenizer.nextToken();
				 // If the current token matches one of the keywords, emit it as a key
        		// and the filename as the corresponding value
				if (args.contains(nxtToken)) {
					output.collect(new Text(nxtToken), new Text(curFileName));
				}
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		// This is the Reduce function used in a MapReduce job
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws
				IOException {
			// Create a HashMap to store the count of occurrences of 'key' in each file.
			HashMap<String, Integer> countMap = new HashMap<String, Integer>();
			//Count the occurrence number of key in each file
			while (values.hasNext()) {
				String queryname = values.next().toString();
				if (countMap.containsKey(queryname)) {
					countMap.put(queryname, countMap.get(queryname) + 1);
				} else {
					countMap.put(queryname, 1);
				}
			}
			//create Comparator to sort the result by count number
			Comparator<java.util.Map.Entry<String, Integer>> resultComparator =
					new Comparator<java.util.Map.Entry<String, Integer>>() {
						@Override
						public int compare(java.util.Map.Entry<String, Integer> e1, java.util.Map.Entry<String, Integer> e2) {
							int value1 = e1.getValue();
							int value2 = e2.getValue();
							return value1 - value2;
						}
					};
			
			//sort the results based on the count
			List<java.util.Map.Entry<String, Integer>> listSortDoc =
					new ArrayList<java.util.Map.Entry<String, Integer>>(countMap.entrySet());
			Collections.sort(listSortDoc, resultComparator);
			
			// Create an output string containing the sorted results
			StringBuilder resultString = new StringBuilder();
			for (java.util.Map.Entry<String, Integer> itr : listSortDoc) {
				resultString.append(itr.getKey());
				resultString.append(" ");
				resultString.append(itr.getValue());
				resultString.append(" ");
			}
			
			// Prepare the result as a Text object
			Text resultTextObj = new Text(resultString.toString());
			// Emit the 'key' and the sorted results as the final output
			output.collect(key, resultTextObj);
		}
	}
	
	public static void main(String[] args) throws Exception {

		// Get the current time to measure the job execution time
		long time = System.currentTimeMillis();
		
		// Create a new JobConf object for configuring the MapReduce job
		JobConf jobConfObj = new JobConf(InvertedIndexing.class);
		jobConfObj.setJobName("invertIndex");
		
		jobConfObj.setOutputKeyClass(Text.class);
		jobConfObj.setOutputValueClass(Text.class);
		
		// Set the mapper and reducer classes for the job.
		jobConfObj.setMapperClass(Map.class);
		
		jobConfObj.setReducerClass(Reduce.class);
		
		// Specify the input and output data formats
		jobConfObj.setInputFormat(TextInputFormat.class);
		jobConfObj.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobConfObj, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConfObj, new Path(args[1]));
		
		jobConfObj.set("argc", String.valueOf(args.length - 2)); 
		for (int itr = 0; itr < args.length - 2; itr++) {
			jobConfObj.set("keyword" + itr, args[itr + 2]);
		}
		
		JobClient.runJob(jobConfObj);
		
		// Calculate and print the elapsed time for job execution
		System.out.println("Elapsed time = " + (System.currentTimeMillis() - time) + " ms");
	}
}
