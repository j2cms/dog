package kmodes;

import instance.Instance;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import util.HDFSUtil;


public class Center {

	public static void initial(String outputBasePath,String centerPath) throws Exception{
		
		ClusterArrayList clusters = new ClusterArrayList();
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		FSDataInputStream in = hdfs.open(new Path(centerPath));
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		
		String strLine = new String();
		while ((strLine = br.readLine()) != null) {
			Instance inst = new Instance(strLine);
			Cluster cluster = new Cluster(clusters.size(),inst);
			clusters.add(cluster);
		}
		in.close();
		
		// 将中心点信息写入文件
		String path = outputBasePath +"/"+  0 +"/centerPoint";
		HDFSUtil.write(path, clusters);
		
		
		
	}
	
	public static ClusterArrayList renew(String outputBasePath, int iter, Configuration conf) throws Exception {
		ClusterArrayList clusters = new ClusterArrayList();
	
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		
		String path =outputBasePath +"/"+  iter +"/center";
		System.out.println("3.path = "+path);
		
		FileStatus[] inputFiles = hdfs.listStatus(new Path(path));
		for (int i = 0; i < inputFiles.length; i++) {
			ClusterArrayList cs = new ClusterArrayList();
			HDFSUtil.read(inputFiles[i].getPath(), cs);
			
//			System.out.println("cs,"+i+"=================\n");
//			for(Cluster c:cs)
//				System.out.println(c);
			
			clusters.merge(cs);
		}
		clusters.renew();
		
		System.out.println("3=================\n clusters.d="+clusters.d);
//		for(Cluster c:clusters)
//			System.out.println(c);
		
		
		return clusters;
	}
	
}
