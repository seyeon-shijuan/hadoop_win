package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//지정 파일을 읽어서 설정대로 매퍼와 리듀서로 워드카운트를 하고 out파일로 내보내는 드라이버 클래스
// 하는 일 : 잡 객체 생성, 잡 객체에 맵리듀스 잡의 실행정보를 설정, 맵리듀스 잡을 실행한다.
public class WordCountEx1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String in = "infile/in.txt";
		String out = "outfile/wordcnt";
		Job job = new Job(conf, "WordCountEx1"); //작업 설정 : 이거
		job.setJarByClass(WordCountEx1.class); //작업 클래스 설정
		job.setMapperClass(WordCountMapper.class);//맵클래스 설정
		job.setReducerClass(WordCountReducer.class); //리듀서클래스 설정
		job.setInputFormatClass(TextInputFormat.class);
		//원본 데이터의 자료형 : 문자형
		job.setOutputFormatClass(TextOutputFormat.class);
		//결과 데이터의 자료형 : 문자형
		job.setMapOutputKeyClass(Text.class);
		//key의 자료형 : 문자형
		job.setMapOutputValueClass(IntWritable.class);
		//value의 자료형 : 정수형
		FileInputFormat.addInputPath(job, new Path(in)); //String in = "infile/in.txt";
		FileOutputFormat.setOutputPath(job, new Path(out)); //String out = "outfile/wordcnt";
		job.waitForCompletion(true); //작업실행
		
	}

}
