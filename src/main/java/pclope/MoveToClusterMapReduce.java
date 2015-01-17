package pclope;

import instance.Instance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.ClusterUtil;
import cluster.ClusterArrayList;

public class MoveToClusterMapReduce {

//	public static enum Counter {
//		MOVED_COUNTER_IN_MAP, MOVED_COUNTER_IN_REDUCE, TIME_COST_HDFS
//	}

	public static class MoveToClusterSingleMapper extends Mapper<LongWritable, Text, IntWritable, Instance> {
		int move = 0;
		long timeCostHDFS;
		double repulsion ;
		long n;

		public ClusterArrayList clusters = new ClusterArrayList();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			repulsion = Double.valueOf( conf.get("repulsion"));
			n = conf.getLong("n", 1L);
			move = 0;
			timeCostHDFS = 0;
			long begin = System.currentTimeMillis();

			String outputBasePath = conf.get("outputBasePath");
			Integer iter = Integer.valueOf(conf.get("iter")) - 1;//是上一次的
			
			//策略二
//			FileSplit fileSplit = (FileSplit) context.getInputSplit();
//			String path = fileSplit.getPath().toString();
//			System.out.println("path = " + path);
//			String id = path.substring(path.lastIndexOf("-") + 1);
//			String clusterPath = outputBasePath + "/clustering/" + iter + "/0" + id;
//			
			
			//策略一
			String clusterPath = outputBasePath + "/clustering/" + iter +"/"+conf.get("bestClusterId");

					
					
					
			System.out.println("clusterPath = " + clusterPath);

			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(new Path(clusterPath));// 打开本地输入流
			clusters.readFields(in);
			in.close();

			timeCostHDFS += (System.currentTimeMillis() - begin);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String tokens[] = value.toString().trim().split("\t");
			int oldClusterId = Integer.parseInt(tokens[0]);
			Instance inst = new Instance(oldClusterId, tokens[1]);

			int newClusterId = Clope.moveInstanceToBestCluster(inst, clusters,repulsion);

			if (oldClusterId != newClusterId) {
				move++;
			}
			context.write(new IntWritable(newClusterId), inst);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
//			long begin = System.currentTimeMillis();

			ClusterUtil.writeCluterToHDFS(context, clusters, move,repulsion,n);

//			timeCostHDFS += (System.currentTimeMillis() - begin);
//			context.getCounter(Counter.TIME_COST_HDFS).increment(timeCostHDFS);
			// context.getCounter(Counter.MOVED_COUNTER_IN_MAP).increment(move);
		}
	}

	

	/**
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @param iter
	 * @return 移动次数
	 * @throws Exception
	 */
	public static void job(Configuration conf, Path input, Path output, int iter, long count) throws Exception {

		Job job = new Job(conf, "CLOPE Phase 2,iter= " + iter);
		job.setMapperClass(MoveToClusterSingleMapper.class);

		job.setNumReduceTasks(0);

		job.setJarByClass(MoveToClusterSingleMapper.class);

		// job.setMapOutputKeyClass(LongWritable.class);
		// job.setMapOutputValueClass(Instance.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Instance.class);

		// job.setInputFormatClass(KeyValueTextInputFormat.class);
		// job.setInputFormatClass(NewTextInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		// job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
		// long movedCounterInMap =
		// job.getCounters().findCounter(Counter.MOVED_COUNTER_IN_MAP).getValue();
		// long movedCounterInReduce =
		// job.getCounters().findCounter(Counter.MOVED_COUNTER_IN_REDUCE).getValue();
//		long timeCostHDFS = job.getCounters().findCounter(Counter.TIME_COST_HDFS).getValue();
		// return new Long[] { timeCostIPC, movedCounterInMap,
		// movedCounterInReduce };
//		return timeCostHDFS;
	}
}
