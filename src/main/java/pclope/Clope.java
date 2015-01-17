package pclope;


import instance.Instance;

import java.util.List;


import cluster.Cluster;
import cluster.ClusterArrayList;

public class Clope {
	public static int addInstanceToBestCluster(Instance inst, List<Cluster> clusters, double repulsion) {
		double delta;
		double deltaMax;
		int clusterId = -1;
		if (clusters.size() > 0) {
			int tempS = 0;
			int tempW = 0;
			for (int i = 0; i < inst.size(); i++) {
				tempS++;
				tempW++;
			}
			deltaMax = tempS / Math.pow(tempW, repulsion);
			for (int id = 0; id < clusters.size(); id++) {
				Cluster tempcluster = clusters.get(id);// clusters.get(id);
				delta = tempcluster.deltaAdd(inst, repulsion);
				if (delta >= deltaMax) {
					deltaMax = delta;
					clusterId = id;
				}
			}
		}
		if (clusterId == -1) {// add
			clusterId = clusters.size();
			inst.setClusterId(clusterId);
			Cluster newCluster = new Cluster();
			newCluster.addInstance(inst);
			clusters.add(newCluster);// clusters.add(newcluster);
		} else {
			inst.setClusterId(clusterId);
			clusters.get(clusterId).addInstance(inst);
		}
		return clusterId;
	}

	public static int moveInstanceToBestCluster(Instance inst, ClusterArrayList clusters, double repulsion) {
		clusters.get(inst.getClusterId()).removeInstance(inst);
		double delta = 0;
		int size = clusters.size();
		int newClusterId = size;
		int tempS = 0;
		int tempW = 0;
		for (int i = 0; i < inst.size(); i++) {
			tempS++;
			tempW++;
		}
		double deltaMax = tempS / Math.pow(tempW, repulsion);
		for (int i = 0; i < size; i++) {
			Cluster c = clusters.get(i);// clusters.get(i);
			delta = c.deltaAdd(inst, repulsion);
			if (delta >= deltaMax) {
				deltaMax = delta;
				newClusterId = i;
			}
		}
		inst.setClusterId(newClusterId);
		if (newClusterId == size) {// new
			System.out.println("产生了新cluster!!!");
			Cluster c = new Cluster();
			c.addInstance(inst);
			clusters.add(c);
		} else {// move
			clusters.get(newClusterId).addInstance(inst);
		}
		return newClusterId;
	}

	
	
}
