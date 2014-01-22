package practice.two;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class WheatDatePriceVO implements Writable{

	private Date minDate;
	private Date maxDate;
	private float minPrice;
	private float maxPrice;
	private float avgPrice;
	private long count;
	
	@Override
	public String toString(){
		return minDate+"--"+maxDate+"--"+minPrice+":"+maxPrice+":"+avgPrice+":"+count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		minDate = new Date(in.readLong());
		maxDate = new Date(in.readLong());
		minPrice = in.readFloat();
		maxPrice = in.readFloat();
		avgPrice = in.readFloat();
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(minDate.getTime());
		out.writeLong(maxDate.getTime());
		out.writeFloat(minPrice);
		out.writeFloat(maxPrice);
		out.writeFloat(avgPrice);
		out.writeLong(count);
	}


	public Date getMinDate() {
		return minDate;
	}

	public void setMinDate(Date minDate) {
		this.minDate = minDate;
	}

	public Date getMaxDate() {
		return maxDate;
	}

	public void setMaxDate(Date maxDate) {
		this.maxDate = maxDate;
	}

	public float getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(float minPrice) {
		this.minPrice = minPrice;
	}

	public float getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(float maxPrice) {
		this.maxPrice = maxPrice;
	}

	public float getAvgPrice() {
		return avgPrice;
	}

	public void setAvgPrice(float avgPrice) {
		this.avgPrice = avgPrice;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
}
