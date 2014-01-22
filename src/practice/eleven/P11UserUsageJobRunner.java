package practice.eleven;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class P11UserUsageJobRunner {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			conf.set("join.type", args[1]);
			
			Job job = new Job(conf, "P11 user usage reduce side joiner");
			
			job.setJarByClass(P11UsageValueMapper.class);
			
			MultipleInputs.addInputPath(job, new Path("/mapred-opr-practice/inputs/users.txt"), TextInputFormat.class,P11UserValueMapper.class);
			MultipleInputs.addInputPath(job, new Path("/mapred-opr-practice/inputs/usage.txt"), TextInputFormat.class,P11UsageValueMapper.class);
			
			job.setReducerClass(P11UserUsageValueReducer.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, new Path(args[0]));
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);
			
		}	
	}
}
