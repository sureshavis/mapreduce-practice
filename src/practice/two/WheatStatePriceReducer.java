package practice.two;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WheatStatePriceReducer extends Reducer<Text, WheatDatePriceVO, Text, WheatDatePriceVO>{

	@Override
	public void reduce(Text key, Iterable<WheatDatePriceVO> values, Context context) throws IOException, InterruptedException{
		WheatDatePriceVO datePriceVO = new WheatDatePriceVO();
		long priceSum = 0l, count = 0l;
		
		
		for(WheatDatePriceVO item : values){
			
			if(datePriceVO.getMinDate() == null || datePriceVO.getMinDate().compareTo(item.getMinDate()) > 0)
				datePriceVO.setMinDate(item.getMinDate());
			
			if(datePriceVO.getMaxDate() == null || datePriceVO.getMaxDate().compareTo(item.getMaxDate()) < 0)
				datePriceVO.setMaxDate(item.getMaxDate());
			
			if(datePriceVO.getMinPrice() == 0.0f || datePriceVO.getMinPrice() > item.getMinPrice())
				datePriceVO.setMinPrice(item.getMinPrice());
			
			if(datePriceVO.getMaxPrice() < item.getMaxPrice())
				datePriceVO.setMaxPrice(item.getMaxPrice());
			
			priceSum += ( item.getAvgPrice() * item.getCount() );
			count += item.getCount();
			
		}
		
		datePriceVO.setAvgPrice(priceSum/count);
		datePriceVO.setCount(count);
		
		context.write(key, datePriceVO);
	}
	
}
