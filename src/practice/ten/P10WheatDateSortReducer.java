package practice.ten;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class P10WheatDateSortReducer extends Reducer<Text, Text, Text, NullWritable>{

	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		for(Text value : values){
			context.write(value, NullWritable.get());
		}
	}
}
