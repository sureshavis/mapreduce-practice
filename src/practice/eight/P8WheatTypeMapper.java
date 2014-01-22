package practice.eight;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P8WheatTypeMapper extends Mapper<Object, Text, Text, Text>{
	
	public static final String stateValue = "State";
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String columnValues [] = value.toString().split(",");
		
		if(columnValues != null && columnValues.length == 9 && !columnValues[0].equals(stateValue)){
			context.write(new Text(columnValues[4]), value);
		}
	}
}
