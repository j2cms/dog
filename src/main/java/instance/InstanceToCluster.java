package instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class InstanceToCluster implements Writable {

	private Instance instance;
	
	private int targetClusterId ;
	
	
	
	public InstanceToCluster() {
		super();
	}

	public InstanceToCluster(Instance instance, int targetClusterId) {
		super();
		this.instance = instance;
		this.targetClusterId = targetClusterId;
	}

	public Instance getInstance() {
		return instance;
	}

	public void setInstance(Instance instance) {
		this.instance = instance;
	}

	public int getTargetClusterId() {
		return targetClusterId;
	}

	public void setTargetClusterId(int targetClusterId) {
		this.targetClusterId = targetClusterId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, this.targetClusterId);
		this.instance.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 this.targetClusterId = WritableUtils.readVInt(in);
		 this.instance.readFields(in);
		
	}
}
