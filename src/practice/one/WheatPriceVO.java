package practice.one;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WheatPriceVO implements Writable{

	private int minPrice;
	private int maxPrice;
	private int avgPrice;
	
	public int getMinPrice() {
		return minPrice;
	}
	public void setMinPrice(int minPrice) {
		this.minPrice = minPrice;
	}
	public int getMaxPrice() {
		return maxPrice;
	}
	public void setMaxPrice(int maxPrice) {
		this.maxPrice = maxPrice;
	}
	public int getAvgPrice() {
		return avgPrice;
	}
	public void setAvgPrice(int avgPrice) {
		this.avgPrice = avgPrice;
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(minPrice);
		out.writeInt(maxPrice);
		out.writeInt(avgPrice);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		minPrice = in.readInt();
		maxPrice = in.readInt();
		avgPrice = in.readInt();
	}
	
	@Override
	public String toString() {
		return minPrice+":"+maxPrice+":"+avgPrice;
	}
}
