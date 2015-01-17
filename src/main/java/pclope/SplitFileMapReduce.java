package pclope;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.HDFSUtil;

public class SplitFileMapReduce {

	// 一定要是static
	public static class SplitFileMapper extends Mapper<LongWritable, Text, Text, Text> {

		Text nullText = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, nullText);
		}
	}

	/**
	 * 
	 * @param n
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	public static void job(int n, Path input, Path output) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		FileStatus inputFS = hdfs.getFileStatus(input);
		long splitSize = inputFS.getLen() / n;

		conf.set("mapred.job.tracker", "lenovo0:9001");
		conf.set("mapred.job.priority", "HIGH");// VERY_HIGH,HIGH,NORMAL
		conf.setLong("mapred.min.split.size", splitSize);
		conf.setLong("mapred.max.split.size", splitSize);
		Job job = new Job(conf, "split file to mutil samll files");

		job.setJarByClass(SplitFileMapReduce.class);
		job.setMapperClass(SplitFileMapper.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	public static void main(String args[]) throws Exception {
		int n = 4;// 100k mushroom 4
		String input = "/user/hadoop/clope/mushroom/agaricus-lepiota.data";
		String output = "/user/hadoop/clope/mushroom/split";

		// String input = "/user/hadoop/clope/census/input/USCensus1990.data-pure.txt";
		// String output = "/user/hadoop/clope/census/split";

		HDFSUtil.deleteIfExist(output);
		job(n, new Path(input), new Path(output));
		HDFSUtil.deleteNoUse(output);
	}

}
