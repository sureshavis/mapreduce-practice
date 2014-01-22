package practice.seven;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P7UserDetailsMapper extends Mapper<Object, Text, Text, Text>{

	
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		
		String userId = value.toString().substring(0,4);
		
		context.write(new Text(userId), value);
	}
}
