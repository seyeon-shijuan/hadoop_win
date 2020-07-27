package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable>{
	//map출력값
	private final static IntWritable one = new IntWritable(1);
	//map 출력키
	private DateKey outputKey =new DateKey();
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Airline al = new Airline(value);
		//출발 지연 데이터 출력
		if(al.isDepartureDelayAvailable()) { //출발 지연 대상이면
			if(al.getDepartureDelayTime() > 0) { //지연 출발 비행기
				//출력키 설정
				outputKey.setYear("D,"+al.getYear());
				outputKey.setMonth(al.getMonth());
				
				//출력데이터 생성
				context.write(outputKey, one);
			} else if (al.getDepartureDelayTime() ==0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if (al.getDepartureDelayTime() <0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			} //inner if
			//도착지연 데이터 출력
			if(al.isArriveDelayAvailable()) { //도착 지연 대상이면
				outputKey.setYear("A,"+al.getYear());
				outputKey.setMonth(al.getMonth());
				
				//매퍼에서는 "1988,1",1,1,1,1,1,1,1,1,1.... 이렇게 된게 reducer로 넘어간다.
				context.write(outputKey, one);
			} else if (al.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else { //출발지연 대상이 아니면
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	}
}
