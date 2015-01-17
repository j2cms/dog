package test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class LocalHost {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		 System.out.println(InetAddress.getLocalHost().getHostName());

	}

}
