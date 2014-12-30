package kmodes;

import instance.Instance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.HDFSUtil;

public class KMapper extends Mapper<Text, Text, IntWritable, Text> {

	ClusterArrayList clusters = new ClusterArrayList();
	int iter = 1;
	String outputBasePath;


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		iter = conf.getInt("iter", 1);
		outputBasePath = conf.get("outputBasePath");
		String path = outputBasePath + "/" + (iter - 1) + "/centerPoint";
		// 读取更新后的中心点坐标
		HDFSUtil.read(path, clusters);
	}

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Text v = (iter==1?key:value);
		String[] tokens = v.toString().trim().split("\\,");

		
		
		if (tokens.length > 1) {
			Instance inst = new Instance(tokens, false);
			int clusterId = KModes.addInsToBestFitCluster(inst, clusters);
			context.write(new IntWritable(clusterId), v);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String id = context.getTaskAttemptID().getTaskID().toString();
		id = id.substring(id.lastIndexOf("_") + 1);
		String path =outputBasePath +"/"+  iter +"/center/"+id;
		
		System.out.println("1.path = "+path);
		
		HDFSUtil.write(path, clusters);
		
//		System.out.println("1=================\n");
//		for(Cluster c:clusters)
//			System.out.println(c);
		
		ClusterArrayList clusters2 = new ClusterArrayList();
		HDFSUtil.read(path, clusters2);
		
		System.out.println("2=================\n");
		for(Cluster c:clusters2)
			System.out.println(c);
	}

}