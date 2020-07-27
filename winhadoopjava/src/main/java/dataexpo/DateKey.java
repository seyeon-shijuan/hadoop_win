package dataexpo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
//하둡의 Mapper에서 키로 사용할 클래스
public class DateKey implements WritableComparable<DateKey> {
	private String year;
	private Integer month;
	
	public DateKey() {}
	public DateKey(String year, Integer date) {
		this.year = year;
		this.month = date;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}
	
	//StringBuilder, StringBuffer : 동적 문자를 구현한 클래스
	//이거 쓰는 이유 : 그냥 String으로 쓰면 Heap이 꽉차서 안돼서
	@Override
	public String toString() {
		return (new StringBuilder()).append(year).append(",").append(month).toString();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		year = WritableUtils.readString(in);
		month = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, year);
		out.writeInt(month);
	}
	@Override
	public int compareTo(DateKey key) { //정렬 관련 메서드
		// TODO Auto-generated method stub
		int result = year.compareTo(key.year);
		if (0==result) { //year 값이 동일한 경우
			result = month.compareTo(key.month);
		}
		return result;
	}
	
}
