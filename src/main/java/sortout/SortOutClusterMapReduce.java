package sortout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.HDFSUtil;

public class SortOutClusterMapReduce {

	// map将输入中的value化成IntWritable类型，作为输出的key
	public static class Map extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		Text nullText = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val,nullText);
			}
		}

	}

	public static class Partition extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return Integer.valueOf(key.toString());
		}

	}

	public static void job(String inputFile,String outputDir,int numReduceTask) throws Exception{
		Configuration conf = new Configuration();
		// 这句话很关键
//		conf.set("mapred.job.tracker", "lenovo0:9001");

		Job job = new Job(conf, "generate cluster to view");
		job.setJarByClass(SortOutClusterMapReduce.class);

		// 设置Map和Reduce处理类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(numReduceTask);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(inputFile));
		HDFSUtil.deleteIfExist(outputDir);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		job.waitForCompletion(true);
		HDFSUtil.deleteNoUse(outputDir);
	}

}