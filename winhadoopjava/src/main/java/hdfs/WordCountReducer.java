package hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 리듀서 클래스는 글자와 글자 수로 구성된 입력 파라미터를 받아 글자 수를 합산해 출력한다.

//WordCountReducer 클래스는 Reducer 클래스를 상속받는다. 이때 파라미터는 <입력 키 타입, 입력 값 타입, 출력 키 타입, 출력 값 타입>이다.
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable result = new IntWritable();
	//이거는 Integer이다.
	@Override
	//파라미터는 <입력 키 타입, Iterable<입력 값 타입>, Context 객체>이다.
	//key에 mapper 결과가 넘어온다.
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		//입력 파라미터의 값에 담겨 잇는 글자 수를 합산하기 위해 선언
		for(IntWritable v: values) { //1,1
			sum += v.get();
		}
		result.set(sum);//2
		//출력값 설정
		context.write(key, result); //this,2
		//is,2
		//a,2
		//book,1
		//pen,1
	}
}
