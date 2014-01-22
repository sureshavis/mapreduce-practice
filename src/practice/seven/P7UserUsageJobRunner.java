package practice.seven;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class P7UserUsageJobRunner {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
			String valueStr = new String("u1        e001    1.1.1.1 1357648060      1357648070");
			System.out.println(new String("e001    suresh  24      chennai se      1       10").substring(0, 4));
			System.out.println("--"+valueStr.substring(valueStr.indexOf('e'), valueStr.indexOf(" ", valueStr.indexOf('e'))));
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "P7 User asset usage job");
			job.setJarByClass(P7UserUsageJobRunner.class);
			
			MultipleInputs.addInputPath(job, new Path("/mapred-opr-practice/inputs/users.txt"), TextInputFormat.class, P7UserDetailsMapper.class);
			MultipleInputs.addInputPath(job, new Path("/mapred-opr-practice/inputs/usage.txt"), TextInputFormat.class, P7UsageDetailsMapper.class);
			
			job.setReducerClass(P7UserUsageReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			TextOutputFormat.setOutputPath(job, new Path(args[0]));
			
			job.waitForCompletion(true);
		}	
	}
}
