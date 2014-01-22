package practice.eleven;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class P11UsageValueMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	public void map(Object key,Text values,Context context) throws IOException, InterruptedException{
		String [] columnValues = values.toString().split("\t");
		
		context.write(new Text(columnValues[1]), new Text("Usage_"+values.toString()));
	}
}
