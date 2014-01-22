package practice.five;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNRecordFilterReducer extends Reducer<NullWritable, Text, NullWritable, Text>{

	private TreeMap<Long, Text> topNValuesMap = new TreeMap<Long, Text>();
	private int nValue;
	
	@Override
	public void setup(Context context){
		nValue = Integer.parseInt(context.getConfiguration().get("nvalue"));
	}
	
	@Override
	public void reduce(NullWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		for(Text value : values){
			String [] columnValues = value.toString().split(",");
			
			topNValuesMap.put(Long.parseLong(columnValues[8]) ,value);
			
			if(topNValuesMap.size() > nValue){
				topNValuesMap.remove(topNValuesMap.firstKey());
			}
			
		}
		
		for(Map.Entry<Long,Text> entry : topNValuesMap.descendingMap().entrySet()){
			context.write(NullWritable.get(), entry.getValue());
		}
		topNValuesMap.clear();
	}
	
}
