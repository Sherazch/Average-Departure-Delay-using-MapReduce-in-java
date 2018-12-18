package HadoopAssignment.Task1;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


public class AverageDelayDriver {
	
	public static void main(String[]args) throws Exception{
		
		Configuration config=new Configuration();
		
		Job job=Job.getInstance(config, "Airlines Delay Count");
		job.setJarByClass(AverageDelayDriver.class);
		job.setMapperClass(AverageDelayMapper.class);
		job.setReducerClass(AverageDelayReducer.class);
		//job.setCombinerClass(AverageDelayCombiner.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	

}
