package hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper 클래스 : 맵의 기능을 처리, 원본 데이터를 읽어서 매핑작업을 한다. 결과는 리듀서의 입력값이 된다.


//WordCountMapper 클래스는 매퍼 클래스를 상속받아 구현한다. 이때 파라미터는 <입력 키 타입, 입력 값 타입, 출력 키 타입, 출력 값 타입>이다.
//LongWritale은 Long, Text는 String, IntWritable은 Integer의 제네릭 데이터타입이다.
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	//값이 1인 Integer. 이유: 맵 메서드에서 출력하는 단어의 글자 수가 무조건 1이라서.
	private final static IntWritable one = new IntWritable(1);
	//단순 문자열 변수
	private Text word = new Text();
	
	
	//입력 데이터를 분석하기 위해 매퍼 클래스에 있는 맵 메서드를 재정의한다.
	@Override
	//value : 값 this is a book.
	
	//이 파라미터는 <입력 키 타입, 입력 값 타입, Context 객체>로 구성된다. 위의 출력관련 내용은 안쓴다.
	// Context 객체는 하둡 맵리듀스 시스템과 통신하면서 출력 데이터를 기록하거나, 모니터링에 필요한 상태값이나 메세지를 갱신하는 역할을 한다.
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		//StringTokenizer: String으로 변환된 value를 공백 단위로 구분 해주는 StringTokenizer 객체 선언
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens()) {
			//this
			word.set(itr.nextToken());			//Text 인터페이스의 set 메서드를 이용해 공백으로 구분된 String 값을 설정한다.
			//context의 write 메서드는 매퍼의 출력 데이터에 레코드를 추가한다. 키가 word이고(토큰1개) 값이 one(Interger 1)
			context.write(word, one); //this : 1
			//is, 1
			//a, 1
			//book, 1
		}
	}
}
