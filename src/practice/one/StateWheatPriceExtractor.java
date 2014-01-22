package practice.one;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StateWheatPriceExtractor {

	public static class StateWheatPriceMapper extends Mapper<Object, Text, Text, WheatPriceVO>{
		
		private static final String excludeColName = "State";
		
		@Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String [] columnValues = value.toString().split(",");
			WheatPriceVO priceVO = new WheatPriceVO();
			
			
			if(columnValues != null && columnValues.length == 9 && !columnValues[0].equals(excludeColName)){
				try{
					priceVO.setMinPrice(Integer.parseInt(columnValues[6]));
				}catch(NumberFormatException e){
					priceVO.setMinPrice(-1);
				}
				
				try{
					priceVO.setMaxPrice(Integer.parseInt(columnValues[7]));
				}catch(NumberFormatException e){
					priceVO.setMaxPrice(-1);
				}
				
				try{
					priceVO.setAvgPrice(Integer.parseInt(columnValues[8]));
				}catch(NumberFormatException e){
					priceVO.setAvgPrice(-1);
				}
				
				//System.out.println("****"+columnValues[0]+"--->"+priceVO);
				context.write(new Text(columnValues[0]), priceVO);
			}
		}
	}
	
	
	public static class StateWheatPriceReducer extends Reducer<Text, WheatPriceVO, Text, WheatPriceVO>{
		
		@Override
		public void reduce(Text key,Iterable<WheatPriceVO> values,Context context) throws IOException, InterruptedException{
			WheatPriceVO finalPriceVO = new WheatPriceVO();
			long sumOfMinPrice =0l, sumOfMaxPrice =0l, sumOfAvgPrice =0l;
			long countOfMinEntry =0l, countOfMaxEntry =0l, countOfAvgEntry =0l;
			
			for( WheatPriceVO priceVO :values){
				if(priceVO.getMinPrice() != -1){
					sumOfMinPrice += priceVO.getMinPrice();
					countOfMinEntry++;
				}
				if(priceVO.getMaxPrice() != -1){
					sumOfMaxPrice += priceVO.getMaxPrice();
					countOfMaxEntry++;
				}
				if(priceVO.getAvgPrice() != -1){
					sumOfAvgPrice += priceVO.getAvgPrice();
					countOfAvgEntry++;
				}		
				//System.out.println("####"+key+"---->"+sumOfAvgPrice+"-"+countOfAvgEntry+":"+sumOfMinPrice+"-"+countOfMaxEntry+":"+sumOfMaxPrice+"-"+countOfMaxEntry);
			}
			
			finalPriceVO.setMinPrice((int) (sumOfMinPrice/countOfMinEntry));
			finalPriceVO.setMaxPrice((int) (sumOfMaxPrice/countOfMaxEntry));
			finalPriceVO.setAvgPrice((int) (sumOfAvgPrice/countOfAvgEntry));
			
			context.write(key, finalPriceVO);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args == null || args.length< 1){
			System.out.println("hdfs output path not available....");
		}else{	
			Configuration conf = new Configuration();
			
			Job job = new Job(conf, "wheat1");
			
			job.setJarByClass(StateWheatPriceExtractor.class);
			
			job.setMapperClass(StateWheatPriceMapper.class);
			job.setReducerClass(StateWheatPriceReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(WheatPriceVO.class);
			
			FileInputFormat.addInputPath(job,new Path("/mapred-practice/wheat/agmarkwheat2012.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			
			job.waitForCompletion(true);
		}	
		
	}

}
