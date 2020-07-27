package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//파일을 여러개 출력을 하도록 설정되어있음
public class DelayCountReducerWithDateKey  extends Reducer<DateKey, IntWritable, DateKey, IntWritable>{
	
	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);//여러개의 파일 생성
	}
	//DateKey가 리듀스의 키값이어야한다. year 앞에는 D, A 이런게 있다.
	public void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String[] columns = key.getYear().split(",");
		int sum = 0;
		Integer bMonth = key.getMonth();
		if(columns[0].equals("D")) {
			for(IntWritable value : values) {
				if(bMonth != key.getMonth()) {
					result.set(sum);
					outputKey.setYear(key.getYear().substring(2)); //D,1988로 써져있기 때문에 3번째인거면 1앞이 잘린다.
					outputKey.setMonth(bMonth);
					mos.write("departure", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}//for
			if(key.getMonth() == bMonth) {
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(key.getMonth());
				result.set(sum);
				mos.write("departure", outputKey, result);
			}
		} else { //columns[0].equals("A") 도착지연 정보
			for (IntWritable value : values) {
				if(bMonth != key.getMonth()) {
					result.set(sum);
					outputKey.setYear(key.getYear().substring(2));
					outputKey.setMonth(bMonth);
					mos.write("arrival", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			if(key.getMonth() == bMonth) {
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(key.getMonth());
				result.set(sum);
				mos.write("arrival", outputKey, result);
			} //if
		}//outer if's else
	}//reduce

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
