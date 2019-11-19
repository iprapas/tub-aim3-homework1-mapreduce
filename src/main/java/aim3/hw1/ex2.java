package aim3.hw1;

/*
 * The base of the code has been taken from my team's implementation of
 * the Map Reduce Lab Exercise by Sergi Nadal
 * in the MSc Big Data Management course in UPC Barcelona
 * The team members were me (Ioannis Prapas) and Ankush Sharma
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ex2 extends JobMapReduce {

	public ex2() {
		this.input = null;
		this.output = null;
	}

	@SuppressWarnings("Duplicates")
	public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Obtain the parameters sent during the configuration of the job
			String select = context.getConfiguration().getStrings("select")[0];
			String project = context.getConfiguration().getStrings("project")[0];
			String[] arrayValues = value.toString().split("\\|");
			context.write(new Text(Utils.getCustomerAttribute(arrayValues, select)), new Text(Utils.getCustomerAttribute(arrayValues, project)+"|cust"));
		}
	}

	@SuppressWarnings("Duplicates")
	public static class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Obtain the parameters sent during the configuration of the job
			String select = context.getConfiguration().getStrings("select")[0];
			String[] arrayValues = value.toString().split("\\|");
			context.write(new Text(Utils.getOrderAttribute(arrayValues, select)), new Text(" "+"|ord"));
		}
	}
	
	public static class AggregationSumReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String temp="";
			for (Text value : values) {
				temp = value.toString();
				if (temp.split("\\|")[1].equalsIgnoreCase("ord")) {
					return;
				}
			}
			context.write(key, new Text(temp.split("\\|")[0]));
		}
	}

	public boolean run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "ex2");
		ex2.configureJob(job, args[1], args[2],  this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

	public static void configureJob(Job job, String pathIn1,String pathIn2, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {

	    job.setJarByClass(ex2.class);
        // Set the reducer class it must use
        job.setReducerClass(AggregationSumReducer.class);
        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // The files the job will read from/write to
		MultipleInputs.addInputPath(job,new Path(pathIn1),TextInputFormat.class,CustomerMapper.class);
		MultipleInputs.addInputPath(job,new Path(pathIn2),TextInputFormat.class,OrderMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(pathOut));
		// These are the parameters that we are sending to the job
        job.getConfiguration().setStrings("select", "custkey");
        job.getConfiguration().setStrings("project", "name");
    }
}
