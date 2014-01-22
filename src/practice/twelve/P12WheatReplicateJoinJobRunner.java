package practice.twelve;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class P12WheatReplicateJoinJobRunner {
	

	public static class P12ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {

		private HashMap<String, String> stateLangMap = new HashMap<String, String>();

		private Text outvalue = new Text();
		private String joinType = null;

		
		
		@Override
		public void setup(Context context) throws IOException,InterruptedException {
			
			try{
				
				Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

				if (files == null || files.length == 0) {
					throw new RuntimeException("state-language.txt is not set in DistributedCache");
				}


				for (Path p : files) {
					BufferedReader rdr = new BufferedReader(
							new InputStreamReader(new FileInputStream(
											new File(p.toString()))));

					String line;
					// For each record in the user file
					while((line = rdr.readLine()) != null) {
						
						String [] columnValues = line.split("=");  
						stateLangMap.put(columnValues[0], columnValues[1]);
					}
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			joinType = context.getConfiguration().get("join.type");
		}
		
		

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String [] columnValues = value.toString().split(",");

			String language = stateLangMap.get(columnValues[0]);

			if (language != null) {
				outvalue.set(language);
				context.write(value, outvalue);
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// If we are doing a left outer join, output the record with an
				// empty value
				context.write(value, new Text(""));
			}
		}
	}

	
	
	public static void main(String[] args) throws Exception {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Replicated Join");
			
			job.getConfiguration().set("join.type", args[1]);
			job.setJarByClass(P12WheatReplicateJoinJobRunner.class);
	
			job.setMapperClass(P12ReplicatedJoinMapper.class);
			job.setNumReduceTasks(0);
	
			TextInputFormat.setInputPaths(job, new Path("/mapred-opr-practice/inputs/agmarkwheat2012.csv"));
			TextOutputFormat.setOutputPath(job, new Path(args[0]));
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
	
			DistributedCache.addCacheFile(new Path("/mapred-opr-practice/inputs/state-language.txt").toUri(),job.getConfiguration());
			//DistributedCache.setLocalFiles(job.getConfiguration(), "/mapred-opr-practice/inputs/state-langauge.txt");
	
			System.exit(job.waitForCompletion(true) ? 0 : 3);
		}	
	}
}