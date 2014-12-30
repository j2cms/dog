package instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class Instance implements WritableComparable<Object>, Cloneable {
	protected int clusterId;
	protected String key;
	protected int itemSize;
	protected ArrayList<String> itemList;

	public Instance() {
		key = new String();
	}

	/**
	 * Add 时有是否带编号的选择
	 * 
	 * @param tokens
	 * @param number
	 */
	public Instance(String[] tokens, boolean number) {
		this.clusterId = -1;
		this.key = tokens[0];
		itemList = new ArrayList<String>();
		if (number)
			for (int i = 1; i < tokens.length; i++)
				// 同一维度才能相比，所以要加上维度编号
				itemList.add(new String(tokens[i] + "(" + i + ")"));
		else
			for (int i = 1; i < tokens.length; i++)
				itemList.add(new String(tokens[i]));

		this.itemSize = this.itemList.size();
	}

	/**
	 * Add时,从map读入一行时构造一个实例,带编号
	 * 
	 * @param line
	 */
	public Instance(String line, boolean number) {
		this.clusterId = -1;
		String[] tokens = line.toString().split("\\,");
		this.key = tokens[0];
		itemList = new ArrayList<String>();
		for (int i = 1; i < tokens.length; i++)
			// itemList.add(new String(tokens[i]));
			// 同一维度才能相比，所以要加上维度编号
			itemList.add(new String(tokens[i] + "(" + i + ")"));
		this.itemSize = this.itemList.size();
	}

	/**
	 * Add时,从map读入一行时构造一个实例，不带编号
	 * 
	 * @param line
	 */
	public Instance(String line) {
		this.clusterId = -1;
		String[] tokens = line.toString().split("\\,");
		this.key = tokens[0];
		itemList = new ArrayList<String>();
		for (int i = 1; i < tokens.length; i++)
			itemList.add(new String(tokens[i]));
		this.itemSize = this.itemList.size();
	}

	/**
	 * move时,从map读入一行时构造一个实例
	 * 
	 * @param line
	 */
	public Instance(int clusterId, String line) {
		this.clusterId = clusterId;
		String[] tokens = line.toString().split("\\,");
		this.key = tokens[0];
		itemList = new ArrayList<String>();
		for (int i = 1; i < tokens.length; i++)
			itemList.add(new String(tokens[i]));
		this.itemSize = this.itemList.size();
	}

	public int size() {
		return itemList.size();
	}

	public int getClusterId() {
		return clusterId;
	}

	public void setClusterId(int clusterId) {
		this.clusterId = clusterId;
	}

	public String getKey() {
		return key;
	}

	public ArrayList<String> getItemList() {
		return itemList;
	}

	public void setItemList(ArrayList<String> itemList) {
		this.itemList = itemList;
	}

	public String info() {
		return "Instance info [clusterId=" + clusterId + ", key=" + key + ", itemSize=" + itemSize + ", itemList=" + itemList + "]";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.clusterId = WritableUtils.readVInt(in);
		this.key = WritableUtils.readString(in);
		itemSize = WritableUtils.readVInt(in);
		itemList = new ArrayList<String>();
		for (int i = 0; i < itemSize; i++) {
			String t = WritableUtils.readString(in);
			itemList.add(t);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, this.clusterId);
		WritableUtils.writeString(out, this.key);
		itemSize = itemList.size();
		WritableUtils.writeVInt(out, itemSize);
		for (int i = 0; i < itemSize; i++) {
			WritableUtils.writeString(out, itemList.get(i));
		}

	}

	@Override
	public int compareTo(Object inst) {
		return key.compareTo(((Instance) inst).getKey());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString());
		for (int i = 0; i < itemList.size(); i++)
			sb.append(",").append(itemList.get(i).toString());
		return sb.toString();
	}

	@Override
	public Instance clone() {
		Instance o = null;
		try {
			o = (Instance) super.clone();
			
			o = new Instance(this.toString());
			
			
//			o.key = new String(this.key);
//
//			ArrayList<String> itemList = new ArrayList<String>();
//			for (int i = 0; i < this.itemList.size(); i++) {
//				itemList.set(i, this.itemList.get(i));
//			}
//			
//			o.setItemList(itemList);
			
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return o;
	}

}
