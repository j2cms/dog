package instance;

import java.util.ArrayList;

public class DNS extends Instance {

	/**
	 * Add时,从map读入一行时构造一个实例
	 * 
	 * @param line
	 */
	public DNS(String line) {
		this.clusterId = -1;
		String[] tokens = line.toString().split("\\,");
		this.key = tokens[0];
		itemList = new ArrayList<String>();
		for (int i = 1; i < tokens.length; i++)
			 itemList.add(new String(tokens[i]));
			// 同一维度才能相比，所以要加上维度编号
//			itemList.add(new String(tokens[i] + "(" + i + ")"));
		this.itemSize = this.itemList.size();
	}
}
