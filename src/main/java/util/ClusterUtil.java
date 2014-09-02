package util;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.LineReader;

import cluster.ClusterArrayList;

public class ClusterUtil {

	/**
	 * 读取HDFS上的profit目录下的文件，返回profit最大的那个
	 * 
	 * @param outputBasePath
	 * @param iter
	 * @return
	 * @throws Exception
	 */
	public static String[] getMaxtProfit(String outputBasePath, int iter) throws Exception {

		String[] d = new String[4];

//		long maxProfit = -1;
		double maxProfit = -1;

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		FileStatus[] inputFiles = hdfs.listStatus(new Path(outputBasePath + "/profit/" + iter));
		for (int i = 0; i < inputFiles.length; i++) {
			FSDataInputStream in = hdfs.open(inputFiles[i].getPath());// 打开本地输入流
			// byte[] buffer = new byte[1024];
			// in.read(buffer);
			
//			int k = in.readInt();
//			long profit = in.readLong();
//			int move = in.readInt();
//			if(profit>maxProfit){
//				d[0] = inputFiles[i].getPath().toString();
//				d[0] = iter + "/" + d[0].substring(d[0].lastIndexOf("/") + 1);
//				d[1] = Integer.toString(k);
//				d[2] = Double.toString(profit);
//				d[3] = Integer.toString(move);
//			}
			
			Text line = new Text();
			LineReader lr = new LineReader(in, conf);
			lr.readLine(line);
			String[] tokens = line.toString().split(",");
//			long profit = Long.valueOf(tokens[1]);
			double profit = Double.valueOf(tokens[1]);
			if (profit > maxProfit) {
				maxProfit = profit;
				d[0] = inputFiles[i].getPath().toString();
				d[0] = iter + "/" + d[0].substring(d[0].lastIndexOf("/") + 1);
				d[1] = tokens[0];
				d[2] = tokens[1];
				d[3] = tokens[2];
			}
			in.close();
		}
		return d;
	}

	public static void writeCluterToHDHS(Context context,ClusterArrayList clusters,Integer move,double repulsion) throws IOException {
		String id = context.getTaskAttemptID().getTaskID().toString();
		id = id.substring(id.lastIndexOf("_") + 1);

		Configuration conf = context.getConfiguration();
		FileSystem hdfs = FileSystem.get(conf);
		// 将聚类信息写入文件
		String path = conf.get("outputBasePath") + "/clustering/" + conf.get("iter") + "/" + id;
		System.out.println("path=" + path);
		FSDataOutputStream out = hdfs.create(new Path(path));// 生成HDFS输出流
		clusters.write(out);

		// 将profit等值写入文件
		String profitPath = conf.get("outputBasePath") + "/profit/" + conf.get("iter") + "/" + id;
		out = hdfs.create(new Path(profitPath));
		System.out.println("profitPath=" + profitPath);
		double d[] = clusters.getSizeOfNotEmptyAndProfit(repulsion);

		if(move ==null ) move = 0;
		
//		//先二进制写一遍
//		out.writeLong(d[0]);
//		out.writeLong(d[1]);
//		out.writeInt(move);
		
		//再用文本方式写一遍
		
		System.out.println("d[1]="+d[1]);
		out.writeBytes(d[0] + "," + d[1]+"," + move);
//		if(move!=null)
//			out.writeBytes(","+move);

		out.close();

	}

}
