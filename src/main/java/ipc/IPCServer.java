package ipc;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class IPCServer {

	public static final long IPC_VERSION = 1L;
	
	public static void main(String ...args) throws InterruptedException, IOException{
		ClusteringImpl  service = new ClusteringImpl();
//		Server s = RPC.getServer(service, "0.0.0.0", IPCClient.IPC_PORT, new Configuration());
		Server s = RPC.getServer(service, "0.0.0.0", IPCClient.IPC_PORT, 8,false,new Configuration());
		s.start();
		
//		while(true){
//			Thread.sleep(10000000);
//		}
		
//		s.stop();
		
		Scanner in = new Scanner(System.in);
		while (in.hasNext()) {
			String line = in.nextLine();// .next();
			if (line.equalsIgnoreCase("EXIT")){
				s.stop();
				System.exit(0);
			}
		}
	}

}
