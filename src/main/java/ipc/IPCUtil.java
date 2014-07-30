package ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;


public class IPCUtil {

	public static Clustering clustering = null;

	public static void startProxy() throws Exception{
		ClusteringImpl  service = new ClusteringImpl();
//		Server s = RPC.getServer(service, "0.0.0.0", IPC_PORT, new Configuration());
		Server s = RPC.getServer(service, "0.0.0.0", IPCClient.IPC_PORT, 8,false,new Configuration());
		s.start();
	}
	
	public static Clustering getProxy() throws IOException{
//		if (clustering == null) {
			InetSocketAddress addr = new InetSocketAddress(IPCClient.IPC_SERVER, IPCClient.IPC_PORT);
			clustering = (Clustering) RPC.getProxy(Clustering.class, IPCServer.IPC_VERSION, addr, new Configuration());
//		}
		return clustering;
	}

	public static void stop(VersionedProtocol v) {
		RPC.stopProxy(v);
	}
}
