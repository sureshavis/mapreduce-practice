package practice.five;

import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNRecordFilterMapper extends Mapper<Object, Text, NullWritable, Text>{
	private TreeMap<Long, Text> topNValuesMap = new TreeMap<Long, Text>();
	private static final String excludeColName = "State";
	private int nValue;
	
	@Override
	public void setup(Context context){
		nValue = Integer.parseInt(context.getConfiguration().get("nvalue"));
		System.out.println("######### nvalue="+nValue);
	}
	
	@Override
	public void map(Object key,Text value,Context context){
		
		String [] columnValues = value.toString().split(",");
		if(columnValues != null && columnValues.length == 9 && !columnValues[0].equals(excludeColName)){
			topNValuesMap.put(Long.parseLong(columnValues[8]), value);
			
			if(topNValuesMap.size() > nValue){
				topNValuesMap.remove(topNValuesMap.firstKey());
			}
		}	
	}
	
	
	/*@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		System.out.println("################ total records="+topNValuesMap.size());
		for(Map.Entry<Long, Text> entry : topNValuesMap.entrySet()){
			context.write(NullWritable.get(), entry.getValue());
		}
		topNValuesMap.clear();
	}*/
}
