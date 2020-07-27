package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithDateKey extends Configured implements Tool{

	public static void main(String[] args)  throws Exception {
		// TODO Auto-generated method stub
		String arg[] = {"-D","workType=departure","D:/ubuntushare/dataexpo/1988.csv","outfile/depart-1988"};
		int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), arg);
	}
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Usage: DelayCountWithDateKey <in> <out>");
			System.exit(2);
		}
		Job job = new Job(getConf(), "DelayCountWithDateKey");
		
		//입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//기존 파일 삭제하기 : 하둡시스템의 파일을 제거하기
		FileSystem hdfs = FileSystem.get(getConf());
		if(hdfs.exists(new Path(otherArgs[1]))) {
			hdfs.delete(new Path(otherArgs[1]),true);
			System.out.println("기존 출력파일을 삭제했습니다.");
		}
		//Job 클래스 설정
		job.setJarByClass(DelayCountWithDateKey.class);
		//Mapper 클래스 설정
		job.setMapperClass(DelayCountMapperWithDateKey.class);
		//Reducer 클래스 설정
		job.setReducerClass(DelayCountReducerWithDateKey.class);
		//Mapper에서 사용할 키 설정인데 DateKey를 쓴다.
		//하둡에서쓰러면 DateKey에 WritableComparable라는 인터페이스를 구현해야만 한다. 키로 정렬된다.
		job.setMapOutputKeyClass(DateKey.class); 
		//value를 설정하는건데 IntWritable로 했고 Writable 인터페이스를 구현해야 한다.
		job.setMapOutputValueClass(IntWritable.class);
		//입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//출력키 및 출력값 유형 설정
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		//MultipleOutputs 설정
		//departure: 출발지연 건수정보, arrival : 도착지연 건수 정보는 arrival-r-00000으로 저장한다.
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}

}
