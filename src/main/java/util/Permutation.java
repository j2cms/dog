package util;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * 求排列 permutation
 * 
 * @author hadoop
 * 
 */
public class Permutation {

	/**
	 * 求一个数组的排列，如{1,2,3,4}
	 * 
	 * @param data
	 * @return
	 */
	public static List<List<Integer>> perm(List<Integer> data) {
		if (data == null || data.size() <= 0)
			return null;
		if (data.size() <= 1) {
			ArrayList<List<Integer>> rs = new ArrayList<List<Integer>>();
			rs.add(data);
			return rs;
		}
		List<List<Integer>> result = new ArrayList<List<Integer>>();
		List<Integer> subData = null;
		List<List<Integer>> rs = null;
		for (int i = 0; i < data.size(); i++) {
			subData = getSubList(data, i);
			rs = perm(subData);
			for (int j = 0; j < rs.size(); j++) {
				rs.get(j).add(data.get(i));
				result.add(rs.get(j));
			}
		}
		return result;
	}

	private static List<Integer> getSubList(List<Integer> data, int index) {
		List<Integer> result = new ArrayList<Integer>();
		for (int i = 0; i < data.size(); i++) {
			if (i != index) {
				result.add(data.get(i));
			}
		}
		return result;
	}

	/**
	 * 求n!
	 * 
	 * @param n
	 * @return
	 */
	public static List<List<Integer>> getPermN(int n) {
		ArrayList<Integer> data = new ArrayList<Integer>();
		for (int i = 0; i < n; i++)
			data.add(i);
		return perm(data);
	}


	public static void gMushroom() throws Exception {
		String inputFile = "/user/hadoop/clope/mushroom/agaricus-lepiota.data";
		String splitDir = "/user/hadoop/clope/mushroom/split";
		String intputDir = "/user/hadoop/clope/mushroom/output/input_tmp";
		HDFSUtil.spiltToNFile(inputFile, splitDir, 4);
		HDFSUtil.generatePermFile(splitDir, intputDir);
	}
	
	public static void gcensus() throws Exception {
		String inputFile = "/user/hadoop/clope/census/input/USCensus1990.data-pure.txt";
		String outputDerectory = "/user/hadoop/clope/census/split";
		HDFSUtil.spiltToNFile(inputFile, outputDerectory, 4);
	}

	public static void main(String[] args) throws Exception {
		long t1 = System.currentTimeMillis();
		gMushroom();
//		gcensus();
		long t2 = System.currentTimeMillis();
		System.out.println(t2-t1);
	}

}