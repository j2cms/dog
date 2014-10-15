package main;

import instance.Instance;
import ipc.Clustering;
import ipc.IPCUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Locale;



import org.apache.hadoop.ipc.RPC;

import cluster.Cluster;
import cluster.ClusterArrayList;

public class NativeClope {
	
	public static ClusterArrayList buildClusterer(ArrayList<Instance> data,double repulsion) throws IOException {

		ClusterArrayList clusters = new ClusterArrayList();
		
//		Clustering clustering = IPCUtil.getProxy();
//		clustering.clear();
		
		NumberFormat nf = NumberFormat.getInstance(Locale.CHINA);
		boolean moved;
//		long d[];
		double d[];
		int n = data.size();
		System.out.println("Phase 1...");
		long time1 = System.currentTimeMillis();
		// Phase 1
		for (int i = 0; i < data.size(); i++) {
			Clope.addInstanceToBestCluster(data.get(i),clusters,repulsion);
		}
		long time2 = System.currentTimeMillis();
		d = clusters.getSizeOfNotEmptyAndProfit(repulsion,n);
		
		System.out.println("Phase 1 done, time cost " + nf.format(time2 - time1) + " ms, generate cluster " + clusters.size()+ ", profit = "+d[1]+", "+( (double) d[1])/n);
		
		long t1 = System.currentTimeMillis();
//		clustering.set(clusters);
		long t2 = System.currentTimeMillis();
		System.out.println("Phase 1 IPC cost " + nf.format(t2 - t1) + " ms"+"\n");

		// Phase 2
		int iter = 0;
		do {
			t1 = System.currentTimeMillis();
			int movedCount = 0;
			iter++;
			System.out.println("Phase 2,iter "+iter+"...");
			moved = false;
			for (int i = 0; i < data.size(); i++) {
				Instance inst = data.get(i);
				int oldClusterId =inst.getClusterId(); 
				int newClusterId  = Clope.moveInstanceToBestCluster(data.get(i),clusters,repulsion);
				if (newClusterId != oldClusterId) {
					moved = true;
					movedCount++;
				}
			}
			t2 = System.currentTimeMillis();
			
			d = clusters.getSizeOfNotEmptyAndProfit(repulsion,n);
			
			System.out.println("Phase 2,iter " + iter+" done, movedCount " + movedCount+", time cost "+nf.format(t2-t1)+" ms"+", Not Empty Cluster "+ d[0]+", profit = "+d[1]+", "+((double) d[1])/n+"\n");
		} while (moved);

		long time3 = System.currentTimeMillis();
		System.out.println("Phase 2 total cost " + nf.format(time3 - time2) + " ms");
		
		//将结果写回IPC
		t1 = System.currentTimeMillis();
//		clustering.clear();
//		clustering.set(clusters);
		t2 = System.currentTimeMillis();
		System.out.println("Phase 2 IPC cost " + nf.format(t2 - t1) + " ms");
		
//		RPC.stopProxy(clustering);

		return clusters;
	}
	
	public static void execute(String inputFile, String outputFile,double repulsion) throws Exception{
		ArrayList<Instance> instList = new ArrayList<Instance>();
		String strLine;
		FileInputStream fin = new FileInputStream(inputFile);
		DataInputStream in = new DataInputStream(fin);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		while((strLine = br.readLine()) != null){
			Instance inst = new Instance(strLine);
			instList.add(inst);
		}
		br.close();
		in.close();
		fin.close();
		
		//调用
		ClusterArrayList clusterArrayList = buildClusterer(instList,repulsion);

		FileWriter out = new FileWriter(outputFile);
		BufferedWriter bw = new BufferedWriter(out);
		bw.write("Total number of instances: " + instList.size() + "\n");

		int k_clusterSize = clusterArrayList.size();//.getSize();
		int count = 0;
		for(int j = 0; j < k_clusterSize; j++){
			Cluster cluster = clusterArrayList.get(j);
			int clusterN = cluster.getN();
//			if(clusterN < 1) continue;
			if(clusterN >0)
				count++;
			bw.write("cluster " + j + " , size " + clusterN  + " \n");
//			for(int k = 0; k < clusterSize; k++){
//				Instance inst = cluster.instList.get(k);
//				bw.write(inst.getKey());
//				for(int m = 0; m < inst.getItemList().size(); m++)
//					bw.write("," + inst.getItemList().get(m));
//				bw.write("\n");
//			}
		}
		bw.write("Total number of clusters  " + k_clusterSize + "\n");
		bw.write("Total number of NOT_EMPTY clusters " + count + "\n");
		
		bw.close();
		out.close();
	}
	
	public static void testDNS() throws Exception{
		double repulsion = 1.8;
		String inputFile = "data/dns/input/1.txt";
		String outputFile = "data/dns/output/r=" + repulsion + ".txt";
		execute(inputFile,outputFile,repulsion);
	}
	
	public static void testDNSr() throws Exception{
		double repulsion = 1.8;

		String inputFile = "data/dns/input/1-reverse.txt";
		String outputFile = "data/dns/output/r=" + repulsion + "_r.txt";
		execute(inputFile,outputFile,repulsion);
	}
	
	public static void testMushroom() throws Exception{
		double repulsion = 3.1;
		String inputFile = "data/mushroom/agaricus-lepiota.data";
		String outputFile = "data/mushroom/output/r=" + repulsion + ".txt";
		execute(inputFile,outputFile,repulsion);
	}

	
	public static void main(String[] args) throws Exception{
//		testMushroom();
//		testDNS();
//		testDNSr();
		if ((args.length !=3) || (args[0].equals("-help"))) {
			System.out.println("命令格式:java -jar main.NativeClope input output repulsion");
			System.exit(-1);
		}
		execute(args[0],args[1],Double.valueOf(args[2]));
	}

}