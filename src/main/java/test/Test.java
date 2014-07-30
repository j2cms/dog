package test;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import cluster.Cluster;
import cluster.ClusterArrayList;


public class Test {

	public static void main(String [] args) throws IOException{
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "lenovo0:9001");
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(new Path("/user/hadoop/clope/mushroom/clustering/0/000000"));// 打开本地输入流
		ClusterArrayList clusters = new ClusterArrayList();
		clusters.readFields(in);
		in.close();
		
		for(Cluster c :clusters){
			System.out.println(c.toString());
		}
		
//		IOUtils.class
	}
}
