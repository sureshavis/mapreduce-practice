package practice.ten;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import practice.nine.P9WheatStateFileRunner.P9WheatFileMapper;

public class P10WheatDateSortJobRunner {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "P10 wheat state price sorting - analysis");
			job.setJarByClass(P10WheatDateSortJobRunner.class);
			
			job.setMapperClass(P10WheatDateValueExtractMapper.class);
			
			job.setNumReduceTasks(0);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			TextInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			SequenceFileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			if(job.waitForCompletion(true)){
				Job orderJob = new Job(conf, " P 10 TotalOrderSortingStage");
				orderJob.setJarByClass(P10WheatDateSortJobRunner.class);

				// Here, use the identity mapper to output the key/value pairs in
				// the SequenceFile
				orderJob.setMapperClass(Mapper.class);
				orderJob.setReducerClass(P10WheatDateSortReducer.class);

				// Set the number of reduce tasks to an appropriate number for the
				// amount of data being sorted
				orderJob.setNumReduceTasks(10);

				// Use Hadoop's TotalOrderPartitioner class
				orderJob.setPartitionerClass(TotalOrderPartitioner.class);

				// Set the partition file
				TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),new Path(args[0]+"/_partitions.lst"));

				orderJob.setOutputKeyClass(Text.class);
				orderJob.setOutputValueClass(Text.class);

				// Set the input to the previous job's output
				orderJob.setInputFormatClass(SequenceFileInputFormat.class);
				SequenceFileInputFormat.setInputPaths(orderJob, new Path(args[0]));

				// Set the output path to the command line parameter
				TextOutputFormat.setOutputPath(orderJob, new Path(args[1]));

				// Set the separator to an empty string
				orderJob.getConfiguration().set(
						"mapred.textoutputformat.separator", "");

				// Use the InputSampler to go through the output of the previous
				// job, sample it, and create the partition file
				InputSampler.writePartitionFile(orderJob,new InputSampler.RandomSampler(135134, 10000));

				// Submit the job
				orderJob.waitForCompletion(true) ;
			}
		}	
	}
}