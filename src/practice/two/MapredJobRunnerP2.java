package practice.two;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapredJobRunnerP2 {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "practice 2");
			
			job.setJarByClass(MapredJobRunnerP2.class); 
			
			job.setMapperClass(WheatStatePriceMapper.class);
			job.setCombinerClass(WheatStatePriceReducer.class);
			job.setReducerClass(WheatStatePriceReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(WheatDatePriceVO.class);
			
			FileInputFormat.addInputPath(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			job.waitForCompletion(true);
		}
	}
	
}
