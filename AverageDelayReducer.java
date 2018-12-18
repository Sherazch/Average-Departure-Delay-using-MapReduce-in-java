package HadoopAssignment.Task1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class AverageDelayReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	public void reduce(Text key,Iterable<Text> values,Context context) 
			throws IOException,InterruptedException{
		
		Double delay=0.00,avg;
		int sum=0;
		for(Text i:values) {
			String[] tmp=i.toString().split(",");
			delay+=Double.parseDouble(tmp[0]);
			sum+=Integer.parseInt(tmp[1]);
			
		}
		avg=delay/sum;
		context.write(key,new DoubleWritable(avg));
	}
}
