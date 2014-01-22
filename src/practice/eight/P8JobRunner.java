package practice.eight;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class P8JobRunner {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "P8 User asset usage job");
			job.setJarByClass(P8JobRunner.class);
			
			job.setMapperClass(P8WheatTypeMapper.class);
			job.setReducerClass(P8WheatTypeReducer.class);
			job.setPartitionerClass(P8WHeatTypePartitioner.class);
			
			job.setNumReduceTasks(10);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			job.waitForCompletion(true);
		}	
	}
}
