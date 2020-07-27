package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DistanceMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, LongWritable> {
	//map출력값
	private final static LongWritable distance = new LongWritable();
	//출력값을 1로할게 아니라 연월과 마일값을 더할거라서 parameter를 비워놓는다.
	
	//map 출력키
	private DateKey outputKey =new DateKey();
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Airline al = new Airline(value);
		if(al.isDistanceAvailable()) {
			if(al.getDistance() >0) {
				outputKey.setYear(al.getYear() + "");
				outputKey.setMonth(al.getMonth());
				distance.set(al.getDistance());
				context.write(outputKey, distance); //[1988,1 : 100,2600,2000 ...] 이렇게 쌓인다.
			}
		}
	}
}

