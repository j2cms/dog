package kmodes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ClusterArrayList extends ArrayList<Cluster> implements Writable {

	private static final long serialVersionUID = 1976555646037505629L;
	
	public int d= 0;

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
	
	public void merge(ClusterArrayList clusters){
		if(this.size()==0)
			this.addAll(clusters);
		else{
			for(int i = 0;i<this.size();i++){
				this.get(i).merge(clusters.get(i));
			}
		}
		
	}
	
	public int renew(){
		for(int i = 0;i<this.size();i++){
			this.d+=this.get(i).renew();
		}
		return this.d;
	}
	
}
