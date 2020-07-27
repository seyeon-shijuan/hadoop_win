package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


//파일은 1개만 출력됨
public class DistanceReducerWithDateKey extends Reducer<DateKey, LongWritable, DateKey, LongWritable> {

	private LongWritable result = new LongWritable();
	public void reduce(DateKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		context.write(key, result);
	}//reduce
}
