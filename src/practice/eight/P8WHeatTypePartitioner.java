package practice.eight;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class P8WHeatTypePartitioner extends Partitioner<Text, Text> implements Configurable{

	private Configuration conf = null;
	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		conf = arg0;
	}

	@Override
	public int getPartition(Text key, Text value, int arg2) {
		String columnValues [] = value.toString().split(",");
		
		
		if(columnValues[4].equals("White"))
			return 1;
		else if(columnValues[4].equals("Mill Quality"))
			return 2;
		else if(columnValues[4].equals("Lokwan"))
			return 3;
		else if(columnValues[4].equals("Lok-1"))
			return 4;
		else if(columnValues[4].equals("Local"))
			return 5;
		else if(columnValues[4].equals("Farmi"))
			return 6;
		else if(columnValues[4].equals("Deshi"))
			return 7;
		else if(columnValues[4].equals("147 Average"))
			return 8;
		else if(columnValues[4].equals("Dara"))
			return 9;
		else
			return 0;	
		
		//return 0;
	}

}
