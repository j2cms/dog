package ipc;

import instance.Instance;
import instance.InstanceToCluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cluster.Cluster;
import cluster.ClusterArrayList;

public class IPCClient {

	public static final String IPC_SERVER = "218.193.154.135";
	// public static final String IPC_SERVER = "lenovo9";//
	// public static final String IPC_SERVER = "lenovo8";
	public static final int IPC_PORT = 32121;

	public static void main(String... args) throws IOException {
		InetSocketAddress addr = new InetSocketAddress(IPCClient.IPC_SERVER, IPCClient.IPC_PORT);
		Clustering clustering = (Clustering) RPC.getProxy(Clustering.class, IPCServer.IPC_VERSION, addr, new Configuration());

		Scanner in = new Scanner(System.in);
		while (in.hasNext()) {
			String line = in.nextLine();// .next();
			long time1 = System.nanoTime();// System.currentTimeMillis();
			long time2 = 0;
			String[] params = line.split(" ");
			if (params[0].equalsIgnoreCase("EXIT")) {
				break;
			} else if (params[0].equalsIgnoreCase("getAll")) {
//				clustering.getAll();
				//打印全部
				System.out.println(clustering.getAll());
				time2 = System.nanoTime();
			} else if (line.equals("get")||params[0].equals("1")){
				//可选操作
				System.out.println(clustering.getSummary());
			}else if (line.equals("getprofile")||params[0].equals("getp")){
				//可选操作
				System.out.println(clustering.getProfit());
			}else if (params[0].startsWith("add")) {
				if (params[0].equals("add")) {
					Instance inst = new Instance(params[1]);
					// Cluster c = new Cluster();
					// c.addInstance(inst);
					int size = clustering.addInstanceToNewCluster(inst);// .add(c);
					System.out.println("the instance added to cluster  " + size + " \n");
				} else if (params[0].equals("add2")) {
					Instance inst = new Instance(params[1]);
					Cluster c = new Cluster();
					c.addInstance(inst);

					Cluster[] clusters = new Cluster[2];
					clusters[0] = c;
					clusters[1] = c;

					clustering.set(clusters);

				} else if (params[0].equals("addto")) {
					int clusterId = Integer.valueOf(params[1]);
					Instance inst = new Instance(params[2]);
					inst.setClusterId(clusterId);

					// way 1
					// Cluster c = clustering.get(clusterId);
					// c.AddInstance(inst);
					// size = clustering.set(c);

					// way 2
					clustering.addOrSetInstanceToCluster(inst);

					// System.out.println("local cluster.N :"+c.getN());
					// System.out.println("remote cluster.N :"+clustering.getSizeOfCluster(clusterId));
				} else if (params[0].equals("addto2")) {
					int clusterId = Integer.valueOf(params[1]);
					Instance inst = new Instance(params[2]);
					inst.setClusterId(clusterId);

					InstanceToCluster instanceToCluster = new InstanceToCluster(inst, clusterId);

					System.out.println(instanceToCluster.getTargetClusterId() + "\t" + instanceToCluster.getInstance());

					// clustering.addInstanceToCluster(instanceToCluster);

					// System.out.println("local cluster.N :"+c.getN());
					// System.out.println("remote cluster.N :"+clustering.getSizeOfCluster(clusterId));
				} else if (params[0].equals("remove")) {
					int clusterId = Integer.valueOf(params[1]);
					clustering.remove(clusterId);
				} else if (params[0].equals("clear")) {
					clustering.clear();
				}
			}else if (params[0].equals("get")){
				int clusterId = Integer.valueOf(params[1]);
				Cluster c = clustering.get(clusterId);
				System.out.println(c.toString());
			}else if(params[0].equals("test")){
				for(int i=0;i<10000;i++){
					Cluster c = clustering.get(i);
				}
			}else if (params[0].equals("getlist")){
				ClusterArrayList clusterArrayList = clustering.getClusterArrayList();
			}
			time2 = System.nanoTime();
			NumberFormat nf = NumberFormat.getInstance(Locale.CHINA);
			System.out.println("Time Cost " + nf.format(time2 - time1) + " ns \n");
		}

		RPC.stopProxy(clustering);
	}

}
