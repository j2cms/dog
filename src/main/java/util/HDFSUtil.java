package util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;

public class HDFSUtil {

	/**
	 * 删除两个无关目录
	 * 
	 * @param p
	 * @throws Exception
	 */
	public static void deleteNoUse(String p) throws Exception {
		// conf1.set("mapred.job.tracker", "lenovo0:9001");
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path deletePath = new Path(p + "/_logs");

		// System.out.println("deleteNoUse=" + deletePath);

		hdfs.delete(deletePath, true);// 是否递归删除
		deletePath = new Path(p + "/_SUCCESS");
		hdfs.delete(deletePath, true);
		// boolean isDeleted = hdfs.delete(deletePath, true);
		// System.out.println(isDeleted);
	}

	/**
	 * 删除目录
	 * 
	 * @param path
	 * @throws Exception
	 */
	public static void deletePath(String path) throws Exception {

		FileSystem hdfs = FileSystem.get(new Configuration());
		Path deletePath = new Path(path);

		// System.out.println("deletePath=" + deletePath);

		hdfs.delete(deletePath, true);
		// boolean isDeleted = hdfs.delete(deletePath, true);// 是否递归删除
		// System.out.println(isDeleted);
	}

	/**
	 * 删除这个目录dir下除了file外的其它文件
	 * 
	 * @param dir
	 * @param file
	 * @throws IOException
	 */
	public static void deleteDirExceptFile(String dir, String file) throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration());

		FileStatus[] fileStatuss = hdfs.listStatus(new Path(dir));
		Path deletePath;
		for (int i = 0; i < fileStatuss.length; i++) {
			deletePath = fileStatuss[i].getPath();
			if (!deletePath.getName().equals(file))
				hdfs.delete(deletePath, true);
		}
	}

	/**
	 * 移动目录里文件到userIdbBrand目录,方便统一处理
	 * 
	 * @param ouput
	 * @throws Exception
	 */
	public static void mv(String src, String dst) throws Exception {
		System.out.println("mv_src=" + src);
		System.out.println("mv_dst=" + dst);
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.rename(new Path(src), new Path(dst));

	}

	/**
	 * 移动目录里文件到userIdbBrand目录,方便统一处理
	 * 
	 * @param ouput
	 * @throws Exception
	 */
	public static void copyFromLocalFile(String src, String dst) throws Exception {
		System.out.println("copy_src=" + src);
		System.out.println("copy_dst=" + dst);
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.copyFromLocalFile(new Path(src), new Path(dst));
	}

	/**
	 * 建目录
	 * 
	 * @param ouput
	 * @throws Exception
	 */
	public static void mkdirs(String path) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.mkdirs(new Path(path));
	}

	/**
	 * 检查是否存在某个目录或者文件
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static boolean exists(String path) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		return hdfs.exists(new Path(path));
	}

	/**
	 * 写入文件
	 * 
	 * @param path
	 * @param s
	 * @throws Exception
	 */
	public static void write(String path, String s) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		OutputStream out = fs.create(new Path(path));
		IOUtils.copyBytes(new ByteArrayInputStream(s.getBytes()), out, 4096, true);
		out.close();
	}

	/**
	 * 写入一个对象到文件
	 * 
	 * @param path
	 * @param o
	 * @throws Exception
	 */
	public static void write(String path, Writable o) throws IOException {
		write(new Path(path),o);
	}
	
	public static void write(Path path, Writable o) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream out = fs.create(path);
		o.write(out);
		out.close();
	}
	
	public static Writable read(String path, Writable o) throws IOException {
		return read(new Path(path),o);
	}
	
	public static Writable read(Path path, Writable o) throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration());
		FSDataInputStream in = hdfs.open(path);// 打开本地输入流
		o.readFields(in);
		in.close();
		return o;
	}

	/**
	 * 假如目录存在则删除之
	 * 
	 * @param path
	 * @throws Exception
	 */
	public static void deleteIfExist(String path) throws Exception {
		if (exists(path)) {
			deletePath(path);
		}
	}

	public static void putMerge(FileStatus[] inputFiles, List<Integer> order, String targetHDFSFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		try {
			FSDataOutputStream out = hdfs.create(new Path(targetHDFSFile));// 生成HDFS输出流
			for (int i = 0; i < order.size(); i++) {
				// 打印当前文件路径
				// System.out.println(inputFiles[order.get(i)].getPath());
				FSDataInputStream in = hdfs.open(inputFiles[order.get(i)].getPath());// 打开本地输入流
				byte[] buffer = new byte[256];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);// 通过一个循环来写入
				}
				in.close();
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static long spiltSingleFileToNFile(String inputFile, String outputDir, int n) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		Path path = new Path(inputFile);
		FileStatus inputFS = hdfs.getFileStatus(path);

		long length = inputFS.getLen();
		long splitSize = length / n + 1;
		// System.out.println("length = " + length + ", splitSize = " +
		// splitSize);
		FSDataInputStream in = hdfs.open(new Path(inputFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		FSDataOutputStream[] out = new FSDataOutputStream[n];

		long flag[] = new long[n];

		for (byte i = 0; i < n; i++) {
			flag[i] = i * splitSize;
			out[i] = hdfs.create(new Path(outputDir + "/" + i));
		}
		String strLine;
		long sumByte = 0;
		int ci = 0;
		long lineNum = 0;
		while ((strLine = br.readLine()) != null) {
			lineNum++;
			int bytesRead = strLine.getBytes().length;
			sumByte += bytesRead;
			// System.out.println(sumByte);
			for (int i = n - 1; i >= 0; i--) {
				if (sumByte > flag[i]) {
					ci = i;
					break;
				}
			}
			// out[ci].writeBytes(strLine + "\n"); // (int) (sumByte/splitSize)
			// out[ci].writeUTF(strLine+"\n");
			out[ci].write((strLine + "\n").getBytes("UTF-8")); // (int)
																// (sumByte/splitSize)
		}
		br.close();
		in.close();

		for (byte i = 0; i < out.length; i++) {
			out[i].close();
		}

		return lineNum;

	}

	public static long spiltToNFile(String input, String outputDir, int n) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		Path inputPath = new Path(input);
		FileStatus inputFS = hdfs.getFileStatus(inputPath);

		List<FileStatus> fsList = new ArrayList<FileStatus>();

		long length = 0l;
		if (inputFS.isDir()) {
			FileStatus[] fss = hdfs.listStatus(inputPath);
			for (int i = 0; i < fss.length; i++) {
				length += fss[i].getLen();
				fsList.add(fss[i]);
			}
		} else {
			length = inputFS.getLen();
			fsList.add(inputFS);
		}

		long splitSize = length / n + 1;
		// System.out.println("length = " + length + ", splitSize = " +
		// splitSize);

		long lineNum = 0;
		FSDataOutputStream[] out = new FSDataOutputStream[n];
		long flag[] = new long[n];
		for (byte i = 0; i < n; i++) {
			flag[i] = i * splitSize;
			out[i] = hdfs.create(new Path(outputDir + "/" + i));
		}
		String strLine;
		long sumByte = 0;
		int ci = 0;

		for (int k = 0; k < fsList.size(); k++) {
			FSDataInputStream in = hdfs.open(fsList.get(k).getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));

			while ((strLine = br.readLine()) != null) {
				lineNum++;
				int bytesRead = strLine.getBytes().length;
				sumByte += bytesRead;
				// System.out.println(sumByte);
				for (int i = n - 1; i >= 0; i--) {
					if (sumByte > flag[i]) {
						ci = i;
						break;
					}
				}
				// out[ci].writeBytes(strLine + "\n"); // (int)
				// (sumByte/splitSize)
				// out[ci].writeUTF(strLine+"\n");
				out[ci].write((strLine + "\n").getBytes("UTF-8")); // (int)
																	// (sumByte/splitSize)
			}
			br.close();
			in.close();

		}

		for (byte i = 0; i < out.length; i++) {
			out[i].close();
		}
		return lineNum;

	}

	/**
	 * 将输入文件夹中的文件排列生成n!种组合的文件
	 * 
	 * @param inputDir
	 * @param outputDir
	 * @throws Exception
	 */
	public static void generatePermFile(String inputDir, String outputDir) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
		FileStatus[] inputFiles = hdfs.listStatus(new Path(inputDir));// FileStatus的listStatus()方法获得一个目录中的文件列表
		int n = inputFiles.length;
		List<List<Integer>> permN = Permutation.getPermN(n);

		for (int i = 0; i < permN.size(); i++) {
			List<Integer> orderList = permN.get(i);
			putMerge(inputFiles, orderList, outputDir + "/" + i);
		}
	}

	// public static void generate (String inputFile,String outputDir, int n)
	// throws IOException{
	// Configuration conf = new Configuration();
	// FileSystem hdfs = FileSystem.get(conf); // 获得HDFS文件系统的对象
	// Path path = new Path(inputFile);
	// FileStatus inputFS = hdfs.getFileStatus(path);
	// long length = inputFS.getLen();
	// long splitSize = length/n+1;
	// System.out.println("length = "+length+", splitSize = "+splitSize);
	// List<FileSplit> splits = new ArrayList<FileSplit>();
	// for(int i =0;i<n;i++){
	// splits.add(new FileSplit(path, i*splitSize, splitSize, new String[] {}));
	// }
	//
	// List<List<Integer>> permN = getPermN(n);
	//
	// for (int i = 0; i < permN.size(); i++) {
	// List<Integer> orderList = permN.get(i);
	// HDFSUtil.putMerge(splits, orderList, outputDir + "/" + i);
	// }
	//
	// }
	//
}
