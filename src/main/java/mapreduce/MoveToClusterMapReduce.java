package mapreduce;

import instance.Instance;
import java.io.IOException;

import main.Clope;

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

	public static enum Counter {
		MOVED_COUNTER_IN_MAP, MOVED_COUNTER_IN_REDUCE, TIME_COST_HDFS
	}

	// public static class MoveToClusterMapper extends Mapper<LongWritable,
	// Text, IntWritable, Instance> {
	//
	// Clustering clustering;
	// long timeCostIPC = 0;
	// List<IntWritable> oldClusterIdList;
	// List<Instance> instanceList;
	// List<Instance> instanceAddList;
	//
	// public static ClusterArrayList clusters = new ClusterArrayList();
	//
	// @Override
	// protected void setup(Context context) throws IOException,
	// InterruptedException {
	//
	// timeCostIPC = 0;
	//
	// oldClusterIdList = new ArrayList<IntWritable>();
	// instanceList = new ArrayList<Instance>();
	// instanceAddList = new ArrayList<Instance>();
	//
	// long begin = System.currentTimeMillis();
	// clustering = IPCUtil.getProxy();
	// clusters = clustering.getClusterArrayList();
	// timeCostIPC += (System.currentTimeMillis() - begin);
	// }
	//
	// @Override
	// protected void map(LongWritable key, Text value, Context context) throws
	// IOException, InterruptedException {
	// String tokens[] = value.toString().split("\t");
	// int oldClusterId = Integer.parseInt(tokens[0]);
	// Instance inst = new Instance(oldClusterId, tokens[1]);
	// // System.out.println(inst.info());
	//
	// // Instance inst = value;
	// // int oldClusterId = inst.getClusterId();
	//
	// int newClusterId = Clope.moveInstanceToBestCluster(inst, clusters);
	//
	// if (oldClusterId != newClusterId) {
	// oldClusterIdList.add(new IntWritable(oldClusterId));
	// inst.setClusterId(newClusterId);
	// instanceList.add(inst);
	// if (newClusterId == -1) {
	// instanceAddList.add(inst);
	// }
	// }
	// if (newClusterId != -1)
	// context.write(new IntWritable(newClusterId), inst);
	// }
	//
	// @Override
	// protected void cleanup(Context context) throws IOException,
	// InterruptedException {
	// long begin = System.currentTimeMillis();
	//
	// IntWritable[] newClusterIdArray =
	// clustering.moveInstanceToCluster(oldClusterIdList.toArray(new
	// IntWritable[0]), instanceList.toArray(new Instance[0]));
	// timeCostIPC += (System.currentTimeMillis() - begin);
	// context.getCounter(Counter.TIME_COST_IPC).increment(timeCostIPC);
	// context.getCounter(Counter.MOVED_COUNTER_IN_MAP).increment(instanceList.size());
	//
	// // 将新增加的的ClusterId Instance 写回
	// for (int i = 0; i < newClusterIdArray.length; i++) {
	// instanceAddList.get(i).setClusterId(newClusterIdArray[i].get());
	// context.write(newClusterIdArray[i], instanceAddList.get(i));
	// }
	//
	// IPCUtil.stop(clustering);
	// }
	//
	// }

	public static class MoveToClusterSingleMapper extends Mapper<LongWritable, Text, IntWritable, Instance> {
		int move = 0;
		long timeCostHDFS;
		double repulsion ;

		public ClusterArrayList clusters = new ClusterArrayList();

		protected void setup(Context context) throws IOException, InterruptedException {
			repulsion = Double.valueOf( context.getConfiguration().get("repulsion"));
			move = 0;
			timeCostHDFS = 0;
			long begin = System.currentTimeMillis();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();

			System.out.println("path = " + path);

			//策略二
			String id = path.substring(path.lastIndexOf("-") + 1);
			Configuration conf = context.getConfiguration();
			Integer iter = Integer.valueOf(conf.get("iter")) - 1;//是上一次的
			String outputBasePath = conf.get("outputBasePath");
			String clusterPath = outputBasePath + "/clustering/" + iter + "/0" + id;
			
			
			//策略一
			clusterPath = outputBasePath + "/clustering/" + iter +"/"+conf.get("bestClusterId");

					
					
					
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
			long begin = System.currentTimeMillis();

			ClusterUtil.writeCluterToHDHS(context, clusters, move,repulsion);

			timeCostHDFS += (System.currentTimeMillis() - begin);
			context.getCounter(Counter.TIME_COST_HDFS).increment(timeCostHDFS);
			// context.getCounter(Counter.MOVED_COUNTER_IN_MAP).increment(move);
		}
	}

	// public static class MoveToClusterReducer extends Reducer<IntWritable,
	// Instance, IntWritable, Instance> {
	// int move = 0;
	// long timeCostIPC;
	// Clustering clustering;
	//
	// public static ClusterArrayList clusters = new ClusterArrayList();
	//
	// protected void setup(Context context) throws IOException,
	// InterruptedException {
	// move = 0;
	// clustering = IPCUtil.getProxy();
	// timeCostIPC = 0;
	// long begin = System.currentTimeMillis();
	// clusters = clustering.getClusterArrayList();
	// timeCostIPC += (System.currentTimeMillis() - begin);
	// }
	//
	// @Override
	// protected void reduce(IntWritable key, Iterable<Instance> values, Context
	// context) throws IOException, InterruptedException {
	// for (Instance val : values) {
	// Instance inst = val;
	// int oldClusterId = key.get();// inst.getClusterId();
	// int newClusterId = Clope.moveInstanceToBestCluster(inst, clusters);
	//
	// if (oldClusterId != newClusterId) {
	// move++;
	// if (newClusterId == -1) {
	// newClusterId = clusters.size();
	// }
	// inst.setClusterId(newClusterId);
	// }
	// context.write(new IntWritable(newClusterId), inst);
	// }
	// }
	//
	// protected void cleanup(Context context) throws IOException,
	// InterruptedException {
	// long begin = System.currentTimeMillis();
	// clustering.set(clusters);
	// timeCostIPC += (System.currentTimeMillis() - begin);
	// context.getCounter(Counter.TIME_COST_IPC).increment(timeCostIPC);
	// context.getCounter(Counter.MOVED_COUNTER_IN_REDUCE).increment(move);
	// RPC.stopProxy(clustering);
	// }
	// }

	/**
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @param iter
	 * @return 移动次数
	 * @throws Exception
	 */
	public static Long job(Configuration conf, Path input, Path output, int iter, long count) throws Exception {

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
		long timeCostHDFS = job.getCounters().findCounter(Counter.TIME_COST_HDFS).getValue();
		// return new Long[] { timeCostIPC, movedCounterInMap,
		// movedCounterInReduce };
		return timeCostHDFS;
	}
}
