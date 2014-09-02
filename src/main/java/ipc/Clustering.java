package ipc;

import instance.Instance;
import instance.InstanceToCluster;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import cluster.Cluster;
import cluster.ClusterArrayList;


public interface Clustering extends VersionedProtocol {

//	abstract public int add(Cluster c);
	abstract public int addInstanceToNewCluster(Instance inst);

	abstract public int addOrSetInstanceToCluster(Instance inst);
	
	abstract public int addOrSetInstanceToCluster(Instance inst, int clusterId);
	
	abstract public int clear();
	
	abstract public Text getAll();
	
	abstract public Text getSummary();
	
	abstract public Cluster get(int clusterId);
	
	abstract public int getSize();

	public double getProfit();
	
	abstract public double[] getSizeOfNotEmptyAndProfit(double r );
	
	abstract public int getSizeOfCluster(int clusterId);
	
	abstract public void moveInstanceToCluter(Instance inst,int newClusterId);

	abstract public int removeInstanceFromCluter(Instance inst) ;

	abstract public int remove(Cluster s);
	
	abstract public int remove(int clusterId);
	
//	abstract public int set(Cluster c);
	
	abstract public void add(ClusterArrayList clusterArrayList);
	
	abstract public void set(ClusterArrayList clusterArrayList);

	void set(Cluster[] clusters);
	
	abstract public void moveInstanceToCluster(InstanceToCluster [] instanceToClusterArray);
	
	abstract public ClusterArrayList getClusterArrayList();
	
//	abstract public int addInstanceToCluster(InstanceToCluster instanceToCluster);

	public abstract IntWritable[] moveInstanceToCluster(IntWritable []oldClusterIdArray,Instance[] instanceArray);

}
