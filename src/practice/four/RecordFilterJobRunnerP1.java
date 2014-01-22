package practice.four;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import practice.two.MapredJobRunnerP2;
import practice.two.WheatDatePriceVO;
import practice.two.WheatStatePriceMapper;
import practice.two.WheatStatePriceReducer;

public class RecordFilterJobRunnerP1 {
	
	public static class RecordFilterMapper extends Mapper<Object, Text, NullWritable, Text>{
		private String filterValue;
		
		@Override
		public void setup(Context context){
			filterValue = context.getConfiguration().get("filter_value");
		}
		
		
		@Override
		public void map(Object Key,Text value,Context context) throws IOException, InterruptedException{
			
			String [] columnValues = value.toString().split(",");
			if(columnValues[0].toString().contains(filterValue)){
				context.write(NullWritable.get(), value);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			conf.set("filter_value", args[1]);
			
			Job job = new Job(conf, "filter practice 1");
			
			job.setJarByClass(RecordFilterJobRunnerP1.class); 
			
			job.setMapperClass(RecordFilterMapper.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			
			job.waitForCompletion(true);
		}
	}

}
