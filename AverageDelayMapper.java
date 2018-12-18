package HadoopAssignment.Task1;

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class AverageDelayMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException{
		int day,delay;
		String carrier="";
		String line=value.toString();
		String[] splits = line.split(",");
		//tmp=Integer.parseInt(splits[0]);
		if(isNumber(splits[0])) {
		day= Integer.parseInt(splits[3]);
		carrier=splits[8];
		if(!isNumber(splits[15])) {delay=0;}
		else{delay=Integer.parseInt(splits[15]);}
		context.write(new Text(carrier+","+day), new Text(delay+","+1));
		}
	}
	
	public boolean isNumber(String s)
    {
        for (int i = 0; i < s.length(); i++)
        if (Character.isDigit(s.charAt(i)) 
            == false)
            return false;
 
        return true;
    }

}
