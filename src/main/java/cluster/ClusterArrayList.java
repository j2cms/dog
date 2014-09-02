package cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


@SuppressWarnings("serial")
public class ClusterArrayList extends ArrayList<Cluster> implements Writable {


	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			Cluster c  = new Cluster();
			c.readFields(in);
			this.add(c);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(super.size()); 
		for (int i = 0; i < super.size(); i++) {
			this.get(i).write(out);
		}
	}

	public double[] getSizeOfNotEmptyAndProfit(double r ) {
		int count = 0;
		double profit = 0 ;		
		for (int i = 0; i < this.size(); i++) {
			Cluster c = this.get(i);
			if (c.getN() > 0) {
//				if(c.getW()==0){
//					System.out.println("c.getW()==0|i="+i+","+c.getS()+","+c.getN()+","+c.getW());
//				}
//				if(incre==Double.NaN)//不能进行这种比较
				profit +=  (c.getS() * c.getN() / Math.pow(c.getW(), r));
				System.out.println("c.getW()==0|i="+i+","+c.getS()+","+c.getN()+","+c.getW());
				count++;
			}				
		}
//		System.out.println("profit="+profit);
		return new double[]{count,profit} ;
	}
	
	public int getSizeOfNotEmpty() {
		int count = 0;
		for (int i = 0; i < this.size(); i++) {
			Cluster c = this.get(i);
			if (c.getN() > 0) {
				// System.out.println("Cluster " + i + ":" + clusters.get(i).N
				// +" instances");
				count++;
			}
		}		
		return count ;
	}
	
}
