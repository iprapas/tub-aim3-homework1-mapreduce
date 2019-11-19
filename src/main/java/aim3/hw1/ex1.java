package aim3.hw1;

/*
 * The base of the code has been taken from my team's implementation of
 * the Map Reduce Lab Exercise by Sergi Nadal
 * in the MSc Big Data Management course in UPC Barcelona
 * The team members were me (Ioannis Prapas) and Ankush Sharma
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ex1 extends JobMapReduce {

	public ex1() {
		this.input = null;
		this.output = null;
	}

	@SuppressWarnings("Duplicates")
	public static class AggregationSumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Obtain the parameters sent during the configuration of the job
			String groupBy = context.getConfiguration().getStrings("groupBy")[0];
			String agg = context.getConfiguration().getStrings("agg")[0];
			String selectField = context.getConfiguration().getStrings("selectField")[0];
			String selectValue = context.getConfiguration().getStrings("selectValue")[0];
			// Since the value is a CSV, just get the lines split by |
			String[] arrayValues = value.toString().split("\\|");

			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String dateInString = Utils.getOrderAttribute(arrayValues, selectField);

			Date date = null;
			Date minDate = null;
			try {
				date = formatter.parse(dateInString);
				minDate = formatter.parse(selectValue);
			} catch (ParseException e) {
				e.printStackTrace();
				return;
			}

			String groupByValue = Utils.getOrderAttribute(arrayValues, groupBy);
			double aggValue = Double.parseDouble(Utils.getOrderAttribute(arrayValues, agg));
			// Do the group by and emit it
            if (date.compareTo(minDate)>=0) {
                context.write(new Text(groupByValue), new DoubleWritable(aggValue));
            }
		}
	}
	
	public static class AggregationSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "ex1");
		ex1.configureJob(job, this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

	public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(ex1.class);
        // Set the mapper class it must use
        job.setMapperClass(AggregationSumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        // Set the combiner class it muse use
        job.setCombinerClass(AggregationSumReducer.class);
        // Set the reducer class it must use
        job.setReducerClass(AggregationSumReducer.class);
        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        // The files the job will read from/write to
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
        // These are the parameters that we are sending to the job
        job.getConfiguration().setStrings("groupBy", "orderpriority");
		job.getConfiguration().setStrings("agg", "price");
		job.getConfiguration().setStrings("selectField", "orderdate");
        job.getConfiguration().setStrings("selectValue", "1993-10-01");
    }
}
