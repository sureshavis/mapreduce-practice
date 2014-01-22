package practice.ten;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P10WheatDateValueExtractMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
		String [] columnValues = values.toString().split(",");
		
		context.write(new Text(columnValues[5]), values);
	}
}
