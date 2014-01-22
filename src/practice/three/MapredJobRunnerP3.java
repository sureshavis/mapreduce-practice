package practice.three;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import practice.two.MapredJobRunnerP2;

public class MapredJobRunnerP3 {
	
	
	public static class RecordCountMapper extends Mapper<Object, Text, NullWritable, NullWritable>{
		private static  final String excludeColName = "State"; 
		
		@Override
		public void map(Object key, Text value, Context context){
			String [] columnValues = value.toString().split(",");

			if(columnValues != null && columnValues.length>0 && !columnValues[0].equals(excludeColName)){
				context.getCounter(excludeColName, columnValues[0]).increment(1);
			}
		}
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "practice 3");
			
			job.setJarByClass(MapredJobRunnerP3.class); 
			
			job.setMapperClass(RecordCountMapper.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(NullWritable.class);
			
			FileInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			if(job.waitForCompletion(true) == true){
				
				Collection<String> counters = job.getCounters().getGroupNames();
				System.out.println("Total counter groups= "+counters.size());
				
				for(String grpName : counters){
					Iterator<Counter> counterItr= job.getCounters().getGroup(grpName).iterator();
					System.out.println(grpName+" : counter entries ="+job.getCounters().getGroup(grpName).size());
					
					while(counterItr.hasNext()){
						Counter counterRef = counterItr.next();
						System.out.println("\t\t "+counterRef.getName()+"="+counterRef.getValue());
					}
					
				}
			}
		}	
	}
}
