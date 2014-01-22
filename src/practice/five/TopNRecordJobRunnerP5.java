package practice.five;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNRecordJobRunnerP5 {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			conf.set("nvalue", args[1]);
			
			Job job = new Job(conf, "top n filter p5");
			job.setJarByClass(TopNRecordJobRunnerP5.class);
			
			job.setNumReduceTasks(1);
			
			job.setMapperClass(TopNRecordFilterMapper.class);
			job.setReducerClass(TopNRecordFilterReducer.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			job.waitForCompletion(true);
		}	
	}
}
