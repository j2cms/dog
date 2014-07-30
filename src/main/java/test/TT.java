package test;

import instance.Instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TT {

	public void change(Integer a){
		a = 1;
	}
	public static void main(String ...agrs){
		Integer a =0;
		new TT().change(a);
		System.out.println(a);
		
		List<Instance> instanceList = new ArrayList<Instance>();
		Instance inst = new Instance("1");
		instanceList.add(inst);
		instanceList.add(inst);
		
		//方法一
		Instance is[] = instanceList.toArray(new Instance[0]);
		
		for(Instance i :is){
			System.out.println(i);
		}
//		System.out.println(is.length);
		//方法二
		Instance[] array = new Instance[instanceList.size()];
		instanceList.toArray(array);
		System.out.println("move--");
		for(Instance i:array){
			System.out.println(i);
		}
		
		Map<String,Instance> cls = new HashMap<String,Instance>();
//		cls.put(key, value);
	}
}
