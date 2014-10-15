package mapreduce;


import instance.Instance;

import java.io.IOException;


import main.Clope;

import org.apache.hadoop.conf.Configuration;
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

public class AddToClusterMapReduce {

	public static enum Counter {
		TIME_COST_PHASE1
	}

	public static class AddToClusterMapper extends Mapper<LongWritable, Text, IntWritable, Instance> {
		public ClusterArrayList clusters = new ClusterArrayList();
		double repulsion;
		boolean number = false;
		long n;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 打印输入文件
			String path = ((FileSplit) context.getInputSplit()).getPath().toString();
			System.out.println("path = " + path);
			
			Configuration conf = context.getConfiguration();
			repulsion = Double.valueOf(conf.get("repulsion","2.6"));
			number = conf.getBoolean("number", false);
			n = conf.getLong("n", 1L);
			

			// 测试一下taskID是否和生成的文件的编号是致的，结果发现是一致的
			// String id = context.getTaskAttemptID().getTaskID().toString();
			// id = id.substring(id.lastIndexOf("_") + 1);
			// context.write(new IntWritable(Integer.valueOf(id)), null);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\\,");
			if(tokens.length>1){
				Instance inst = new Instance(tokens,number);
				int clusterId = Clope.addInstanceToBestCluster(inst, clusters, repulsion);
				context.write(new IntWritable(clusterId), inst);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			long begin = System.currentTimeMillis();
			ClusterUtil.writeCluterToHDHS(context, clusters, null, repulsion,n);
			clusters.clear();
			long timeCostHDFS = (System.currentTimeMillis() - begin);
			context.getCounter(Counter.TIME_COST_PHASE1).increment(timeCostHDFS);
		}
	}

	// public static class AddToClusterReducer extends Reducer<IntWritable,
	// Instance, IntWritable, Text> {
	//
	// @Override
	// protected void reduce(IntWritable key, Iterable<Instance> values, Context
	// context) throws IOException, InterruptedException {
	// for (Instance val : values) {
	// context.write(key, new Text(val.toString()));
	// }
	// }
	//
	// // @Override
	// // protected void cleanup(Context context) throws IOException,
	// // InterruptedException {
	// // Configuration conf = context.getConfiguration();
	// // FileSystem fs = FileSystem.get(conf);
	// // Path outCluster = new Path(conf.get("clope.temp.newcluster"));
	// // FSDataOutputStream out = fs.create(outCluster);
	// // out.writeInt(Clope.globalClusters.size());
	// // for (int i = 0; i < Clope.globalClusters.size(); i++)
	// // Clope.globalClusters.get(i).write(out);
	// // out.close();
	// // }
	// }

	public static long job(Configuration conf, Path input, Path output) throws Exception {
		Job job = new Job(conf, "CLOPE Phase 1");

		job.setJarByClass(AddToClusterMapReduce.class);
		job.setMapperClass(AddToClusterMapper.class);
		// job.setReducerClass(AddToClusterReducer.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Instance.class);
		// job.setOutputKeyClass(IntWritable.class);
		// job.setOutputValueClass(Text.class);

		// job.setPartitionerClass(HashPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		// job.setInputFormatClass(NewTextInputFormat.class);
		// job.setInputFormatClass(CombineFileInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		// NewTextInputFormat.addInputPath(job, input);
		// CombineFileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
		long timeCostIPC = job.getCounters().findCounter(Counter.TIME_COST_PHASE1).getValue();
		return timeCostIPC;
	}

}
