package practice.two;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WheatStatePriceMapper extends Mapper<Object, Text, Text, WheatDatePriceVO>{
	
	private static final String excludeColName = "State";
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("mm/dd/yyyy");
	
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		WheatDatePriceVO datePriceVO = new WheatDatePriceVO();
		String columnValues [] = value.toString().split(",");
		
		if(columnValues != null && columnValues.length == 9 && !columnValues[0].equals(excludeColName)){

			datePriceVO.setMinPrice(Float.parseFloat(columnValues[6]));
			datePriceVO.setMaxPrice(Float.parseFloat(columnValues[7]));
			datePriceVO.setAvgPrice(Float.parseFloat(columnValues[8]));
			datePriceVO.setCount(1L);
			
			try{
				Date recordDate = dateFormatter.parse(columnValues[5]);
				datePriceVO.setMinDate(recordDate);
				datePriceVO.setMaxDate(recordDate);
				
				context.write(new Text(columnValues[0]), datePriceVO);
			}catch(ParseException e){
				System.out.println("parsing failed for entry "+columnValues[0]+" on "+columnValues[5]);
			}
			
		}	
	}
	
}
