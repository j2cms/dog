package ipc;

import instance.Instance;
import instance.InstanceToCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import cluster.Cluster;
import cluster.ClusterArrayList;

public class ClusteringImpl implements Clustering {

	// public static List<Text> clusters = new ArrayList<Text>();
	// public static Set<Text> clusters = new HashSet<Text>();

	public static ClusterArrayList clusterArrayList  = new ClusterArrayList();
	
	public static double profit;

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return IPCServer.IPC_VERSION;
	}

//	@Override
//	public synchronized int add(Cluster c) {
//		
//		clusterArrayList.add(c);// add后size+1
//		return newClusterId;
//	}

	
	
	public int addInstanceToNewCluster(Instance inst){
		// 增加到List尾部，则id为当前List的size()
//		int newClusterId = clusterArrayList.size();
//		Cluster newCluster = new Cluster(newClusterId);
		Cluster newCluster = new Cluster();
//		inst.setClusterId(newClusterId);
		newCluster.addInstance(inst);
		clusterArrayList.add(newCluster);//add后size+1
		return clusterArrayList.size() -1 ;
	}


	private void addInstanceToExistCluster(Instance inst,int targetClusterId){
		clusterArrayList.get(targetClusterId).addInstance(inst);
	}
	
	@Override
	public int addOrSetInstanceToCluster(Instance inst) {
		return addOrSetInstanceToCluster(inst,inst.getClusterId());
	}

	// 作用与get()然后set相当,但可以减少IO
	@Override
	public int addOrSetInstanceToCluster(Instance inst, int newClusterId) {
		if((newClusterId==-1)||(newClusterId>clusterArrayList.size()-1)) {//add
			return addInstanceToNewCluster(inst);
		}
//		if ((newClusterId>=0)&&(newClusterId < clusterArrayList.size())){//move
		else{//move
			addInstanceToExistCluster(inst,newClusterId);
			return newClusterId;
		}
	}

	@Override
	public int clear() {
		clusterArrayList.clear();
		return 0;
	}

	@Override
	public int getSize() {
		return clusterArrayList.size();
	}

	public double getProfit() {
		return profit;
	}
	
	@Override
	public double[] getSizeOfNotEmptyAndProfit(double r ,long n) {
		return clusterArrayList.getSizeOfNotEmptyAndProfit(r,n);
	}
	
	public int getSizeOfNotEmpty() {
		return clusterArrayList.getSizeOfNotEmpty();
	}

	@Override
	public int getSizeOfCluster(int clusterId) {
		return clusterArrayList.get(clusterId).getN();
	}

	/**
	 * 直接get()后得到的引用，再修改后不会自动写回，需要再用set()
	 */
	@Override
	public Cluster get(int clusterId) {
		return clusterArrayList.get(clusterId);
	}

	@Override
	public Text getAll() {
		StringBuffer sb = new StringBuffer();
//		for (Cluster c : clusterArrayList) {
//			sb.append(c);
//		}
		int size =clusterArrayList.size();
		for (int i =0;i<size;i++) {
			sb.append(i+" "+clusterArrayList.get(i).toString()+"\n");
		}

		return new Text(sb.toString());
	}

	@Override
	public Text getSummary() {
		int k = this.getSize();
		int n = 0;
		StringBuffer sb = new StringBuffer();
		
		for (int i = 0; i < k; i++) {
			int c = this.getSizeOfCluster(i);
			sb.append("cluster " + i + ", size " + c + "\n");
			n += c;
		}
		sb.append("Total Instance " + n +", Not Empty clusters size " + this.getSizeOfNotEmpty() + "\n");
		return new Text(sb.toString());
	}

	public void moveInstanceToCluter(Instance inst, int newClusterId) {
		clusterArrayList.get(inst.getClusterId()).removeInstance(inst);
		// inst.setClusterId(newClusterId);//不写亦可
		addOrSetInstanceToCluster(inst, newClusterId);
	}
	
	@Override
	public synchronized int removeInstanceFromCluter(Instance inst) {
		clusterArrayList.get(inst.getClusterId()).removeInstance(inst);
		return 0;
	}

	@Override
	public int remove(Cluster s) {
		clusterArrayList.remove(s);
		return clusterArrayList.size();
	}

	@Override
	public int remove(int clusterId) {
		clusterArrayList.remove(clusterId);
		return clusterArrayList.size();
	}

//	@Override
//	public synchronized int set(Cluster c) {
//		clusterArrayList.set(c.getId(), c);
//		return 0;
//	}	
	
	@Override
	public void add(ClusterArrayList clusterArrayListWritable){
			clusterArrayList.addAll(clusterArrayListWritable);
	}
	
	@Override
	public void set(ClusterArrayList clusterArrayListWritable){
			clusterArrayList = clusterArrayListWritable;
	}
	
	@Override
	public void set(Cluster[] clusterArray){
		for(Cluster c:clusterArray){
			clusterArrayList.add(c);
		}
	}

	@Override
	public synchronized void moveInstanceToCluster(InstanceToCluster[] instanceToClusterArray) {
		for(InstanceToCluster instanceToCluster:instanceToClusterArray){
			System.out.println(instanceToCluster.getInstance()+",id="+instanceToCluster.getTargetClusterId());
			moveInstanceToCluter(instanceToCluster.getInstance(),instanceToCluster.getTargetClusterId());
		}
	}

	@Override
	public ClusterArrayList getClusterArrayList() {
		return clusterArrayList;
	}

//	@Override
//	public int addInstanceToCluster(InstanceToCluster instanceToCluster) {
//		addInstanceToCluster(instanceToCluster.getInstance(),instanceToCluster.getTargetClusterId());
//		return 0;
//	}

	@Override
	public synchronized IntWritable[] moveInstanceToCluster(IntWritable []oldClusterIdArray,Instance[] instanceArray) {
//		System.out.println("move "+instanceArray.length);
		List<IntWritable> newClusterIdList = new ArrayList<IntWritable>();
		for(int i=0;i<instanceArray.length;i++){
			// remove
			clusterArrayList.get(oldClusterIdArray[i].get()).removeInstance(instanceArray[i]);
			
			int newClusterId = instanceArray[i].getClusterId();
			if((newClusterId==-1)||(newClusterId>clusterArrayList.size()-1)){//add
				newClusterId = addInstanceToNewCluster(instanceArray[i]);
				newClusterIdList.add(new IntWritable(newClusterId));
			}else{
				addInstanceToExistCluster(instanceArray[i],newClusterId);
			}
		}
		return newClusterIdList.toArray(new IntWritable[0]);
		
	}	
	
	
	
}
