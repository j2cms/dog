package cluster;

import instance.Instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Cluster implements Writable {

//	/**
//	 * id/key of cluster by GT
//	 */
//	private int id = -1;

	/**
	 * Number of transactions
	 */
	private int N = 0;

	/**
	 * Number of distinct items (or width)
	 */
	private int W = 0;

	/**
	 * Size of cluster
	 */
	private int S = 0;

	/**
	 * Hash of <item, occurrence> pairs
	 */
	private HashMap<String, Integer> occ = new HashMap<String, Integer>();

	public Cluster() {
	}
	
	public Cluster(String zkString) {
		String [] tokens = zkString.split(",");
		this.N = Integer.valueOf(tokens[0]);
		this.W = Integer.valueOf(tokens[1]);
		this.S = Integer.valueOf(tokens[2]);
		occ = new HashMap<String, Integer>();
		for(int i =3;i<tokens.length;i++){
			String token[] = tokens[i].split("=");
			occ.put(token[0], Integer.valueOf(token[1]));
		}
	}
	
//	public Cluster(int id) {
//		super();
//		this.id = id;
//	}
//
//	public int getId() {
//		return id;
//	}
//
//	public void setId(int id) {
//		this.id = id;
//	}

	public int getN() {
		return N;
	}

	public void setN(int n) {
		N = n;
	}

	public int getW() {
		return W;
	}

	public void setW(int w) {
		W = w;
	}

	public int getS() {
		return S;
	}

	public void setS(int s) {
		S = s;
	}

	public void addItem(String Item) {
		int count;
		if (!this.occ.containsKey(Item)) {
			this.occ.put(Item, 1);
		} else {
			count = (Integer) this.occ.get(Item);
			count++;
			// this.occ.remove(Item);
			this.occ.put(Item, count);
		}
		this.S++;
	}

	public void removeItem(String Item) {
		int count;

		count = (Integer) this.occ.get(Item);

		if (count == 1) {
			this.occ.remove(Item);

		} else {
			count--;
			// this.occ.remove(Item);
			this.occ.put(Item, count);
		}
		this.S--;
	}

	public double deltaAdd(Instance inst, double r) {
		int S_new;
		int W_new;
		double profit;
		double profit_new;
		double deltaprofit;
		S_new = 0;
		W_new = occ.size();

		for (int i = 0; i < inst.size(); i++) {
			S_new++;

			if (this.occ.get(inst.getItemList().get(i)) == null) {
				W_new++;
			}
		}

		S_new += S;

		if (N == 0) {
			deltaprofit = S_new / Math.pow(W_new, r);
		} else {
			profit = S * N / Math.pow(W, r);
			profit_new = S_new * (N + 1) / Math.pow(W_new, r);
			deltaprofit = profit_new - profit;
		}
		return deltaprofit;
	}

	/**
	 * Add instance to cluster Not actually add the instance, just modify the
	 * S,N and W values
	 */
	public void addInstance(Instance inst) {

		for (int i = 0; i < inst.size(); i++) {
			this.addItem(inst.getItemList().get(i));
			// for(int i=0;i<inst.numAttributes();int++){
			// AddItem(inst.index(i)+inst.value(i));
		}

		this.W = this.occ.size();
		this.N++;
	}

	/**
	 * Delete instance from cluster
	 */
	public void removeInstance(Instance inst) {

		for (int i = 0; i < inst.size(); i++) {
			this.removeItem(inst.getItemList().get(i));
		}
		this.W = this.occ.size();
		this.N--;
	}

	@Override
	public void write(DataOutput out) throws IOException {
//		WritableUtils.writeVInt(out, id);
		WritableUtils.writeVInt(out, S);
		WritableUtils.writeVInt(out, W);
		WritableUtils.writeVInt(out, N);
		for (String item : occ.keySet()) {
			WritableUtils.writeString(out, item);
			WritableUtils.writeVInt(out, occ.get(item));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
//		id = WritableUtils.readVInt(in);
		S = WritableUtils.readVInt(in);
		W = WritableUtils.readVInt(in);
		N = WritableUtils.readVInt(in);
		occ = new HashMap<String, Integer>();
		for (int i = 0; i < W; i++) {
			String t =WritableUtils.readString(in);
			int freq = WritableUtils.readVInt(in);
			occ.put(t, freq);
		}
	}

	@Override
	public String toString() {
		// StringBuffer sb = new StringBuffer();
		// for (String item : occ.keySet()) {
		// sb.append("["+item.toString()+":"+occ.get(item)+"] ");
		// }
		
		return "Cluster [N=" + N + ", W=" + W + ", S=" + S + ", occ=" + occ + "]";	
	}
	
	public String toZKString(){
		StringBuilder sb = new StringBuilder();
		sb.append(N+","+W+","+S);
		for (String item : occ.keySet()) {
			sb.append(","+item.toString()+"="+occ.get(item));
		}
		return sb.toString();
	}
	
	
}
