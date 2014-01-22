package practice.nine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import practice.eight.P8JobRunner;
import practice.eight.P8WHeatTypePartitioner;
import practice.eight.P8WheatTypeMapper;
import practice.eight.P8WheatTypeReducer;

public class P9WheatStateFileRunner {

	
	public static class P9WheatFileMapper extends Mapper<Object, Text, Text, NullWritable>{
		MultipleOutputs<Text, NullWritable> mulOuts ;
		
		@Override
		public void setup(Context context){
			mulOuts = new MultipleOutputs(context);
		}
		
		@Override
		public void map(Object key,Text values,Context context) throws IOException, InterruptedException{
			String [] columnValues = values.toString().split(",");
			
			mulOuts.write("state", values, NullWritable.get(), "state/"+columnValues[0].replaceAll(" ", "_"));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			mulOuts.close();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "P9 wheat state price binning");
			job.setJarByClass(P9WheatFileMapper.class);
			
			job.setMapperClass(P9WheatFileMapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			
			FileInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			//LazyOutputFormat.setOutputFormatClass(job, FileOutputFormat.class);
			
			MultipleOutputs.addNamedOutput(job, "state", TextOutputFormat.class, Text.class	, NullWritable.class);
			MultipleOutputs.setCountersEnabled(job, true);
			
			job.waitForCompletion(true);
		}	
	}
	
}
