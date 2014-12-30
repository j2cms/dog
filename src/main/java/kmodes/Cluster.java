package kmodes;

import instance.Instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

class Column implements Writable {

	// <A,2>
	private Map<String, Integer> occ = new HashMap<String, Integer>();

	// 返回数量最多的那一项作为此列的新属性
	public String getColum() {
		Map.Entry<String, Integer> column = null;
		int max = Integer.MIN_VALUE;
		for (Map.Entry<String, Integer> entry : occ.entrySet()) {
			if (entry.getValue() > max)
				column = entry;
		}
		return column.getKey();
	}

	public void addItem(String Item) {
		if (!this.occ.containsKey(Item)) {
			this.occ.put(Item, 1);
		} else {
			int count = this.occ.get(Item) + 1;
			this.occ.put(Item, count);
		}
	}

	/**
	 * 返回合并后的Column
	 * 
	 * @param column
	 * @return
	 */
	public Column merge(Column column) {
		for (Map.Entry<String, Integer> entry : occ.entrySet()) {
			if (!this.occ.containsKey(entry.getKey())) {
				this.occ.put(entry.getKey(), entry.getValue());
			} else {
				int value = this.occ.get(entry.getKey()) + entry.getValue();
				this.occ.put(entry.getKey(), value);
			}
		}
		return this;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(occ.size());
		for (Map.Entry<String, Integer> entry : occ.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeInt(entry.getValue());
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		occ = new HashMap<String, Integer>();// !
		for (int i = 0; i < size; i++) {
			String t = in.readUTF();
			int freq = in.readInt();
			occ.put(t, freq);
		}

	}

	@Override
	public String toString() {
		return "Column [occ=" + occ + "]";
	}
	
}






public class Cluster implements Writable {

	// 簇的编号
	public int id;

	// 中心点
	public Instance center = new Instance();

	public static int columnNumber;
	Column[] columns = new Column[columnNumber];

	// List<Column> columnList = new ArrayList<Column>();

	public Cluster() {
		super();
		columns = new Column[columnNumber];
		for(int i= 0;i<columnNumber;i++){
			columns[i] = new Column();
		}
	}

	public Cluster(int id, Instance center) {
		super();
		this.id = id;
		this.center = center;

		Cluster.columnNumber = center.size();
		columns = new Column[columnNumber];
		for(int i= 0;i<columnNumber;i++){
			columns[i] = new Column();
		}
	}

	public void addInstance(Instance inst) {
		for (int i = 0; i < inst.size(); i++) {
			columns[i].addItem(inst.getItemList().get(i));
			
//			System.out.println("columns[i]= " + columns[i]);
		}
	}

	public Cluster merge(Cluster cluster) {
		if (this.id != cluster.id)
			System.out.println("id不同无法合并");
		else {
			for (int i = 0; i < columnNumber; i++) {
				this.columns[i].merge(cluster.columns[i]);
			}
		}
		return this;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		center.write(out);
		
//		System.out.print("columnNumber:"+columnNumber);

		out.writeInt(columnNumber);
		for (int i = 0; i < columnNumber; i++) {
			this.columns[i].write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.center.readFields(in);

		columnNumber = in.readInt();
		this.columns = new Column[columnNumber];
		for (int i = 0; i < columnNumber; i++) {
			Column c = new Column();
			c.readFields(in);
			this.columns[i] = c;
		}
	}

	/**
	 * 计算当前实例对象与中心点的差异性
	 * 
	 * @param inst
	 * @return
	 */
	public int different(Instance inst) {
		int d = 0;
		for (int i = 0; i < inst.getItemList().size(); i++) {
			if (!(this.center.getItemList().get(i)).equals(inst.getItemList().get(i)))
				d++;
		}
		return d;
	}

	public int renew() {
		Instance old = this.center.clone();
		for (int i = 0; i < this.center.size(); i++) {
			this.center.getItemList().set(i, columns[i].getColum());
		}
		
		System.out.println("old="+old);
		System.out.println("center="+center);
		
		return this.different(old);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Cluster id=" + id +", columnNumber=" +columnNumber+", center=" + center + "\n");
		for(int i=0;i<columnNumber;i++){
			sb.append(columns[i]+"\n");
		}
//		return "Cluster [id=" + id + ", center=" + center + ", columns=" + columns + "]";
		return sb.toString();
	}

}
