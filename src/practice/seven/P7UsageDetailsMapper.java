package practice.seven;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P7UsageDetailsMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String columnValues [] = value.toString().split("\t");
		System.out.println(columnValues[1]);
		
		context.write(new Text(columnValues[1]), value);
		
	}
}
