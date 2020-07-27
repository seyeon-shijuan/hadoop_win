package hdfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayHadoopFile {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String filepath = "infile/in.txt";
		Path pt = new Path(filepath);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt), "UTF-8"));
		String line = null;
		while(  (line = br.readLine() ) != null){
			System.out.println(line);
		}
		br.close();
	}

}
