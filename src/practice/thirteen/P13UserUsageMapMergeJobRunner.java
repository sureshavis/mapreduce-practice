package practice.thirteen;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.util.GenericOptionsParser;

public class P13UserUsageMapMergeJobRunner {

	public static class P13CompositeMapper extends MapReduceBase implements
			Mapper<Text, TupleWritable, Text, Text> {

		@Override
		public void map(Text key, TupleWritable value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Get the first two elements in the tuple and output them
			output.collect((Text) value.get(0), (Text) value.get(1));
		}
	}

	public static void main(String[] args) throws Exception {

		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{
			JobConf conf = new JobConf("P13 CompositeJoin");
			conf.setJarByClass(P13UserUsageMapMergeJobRunner.class);
			Path userPath = new Path("/mapred-opr-practice/inputs/users.txt");
			Path usagePath = new Path("/mapred-opr-practice/inputs/usage-ext.txt");
			Path outputDir = new Path(args[0]);
			String joinType = args[1];
			if (!(joinType.equalsIgnoreCase("inner") || joinType
					.equalsIgnoreCase("outer"))) {
				System.err.println("Join type not set to inner or outer");
				System.exit(2);
			}

			conf.setMapperClass(P13CompositeMapper.class);
			conf.setNumReduceTasks(0);

			// Set the input format class to a CompositeInputFormat class.
			// The CompositeInputFormat will parse all of our input files and output
			// records to our mapper.
			conf.setInputFormat(CompositeInputFormat.class);

			// The composite input format join expression will set how the records
			// are going to be read in, and in what input format.
			conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType,
					KeyValueTextInputFormat.class, userPath, usagePath));

			TextOutputFormat.setOutputPath(conf, outputDir);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			RunningJob job = JobClient.runJob(conf);
			while (!job.isComplete()) {
				Thread.sleep(1000);
			}

			System.exit(job.isSuccessful() ? 0 : 2);
		}

	}
}
