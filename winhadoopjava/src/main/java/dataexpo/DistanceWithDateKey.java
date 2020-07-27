package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//월별 운항 거리 출력하기
public class DistanceWithDateKey {

	public static void main(String[] args)  throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DistanceWithDateKey");
		//입출력 데이터 경로 설정
		String in = "D:/ubuntushare/dataexpo/1988.csv";
		String out = "outfile/distance-1988";
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out),true);
			System.out.println("기존 출력파일이 삭제되었습니다.");
		}// if
		job.setJarByClass(DistanceWithDateKey.class);
		job.setMapperClass(DistanceMapperWithDateKey.class);
		job.setReducerClass(DistanceReducerWithDateKey.class);
		job.setMapOutputKeyClass(DateKey.class);
		//valueClass가 LongWritable로 된다. (큰 숫자 쓸거라서)
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}

}
