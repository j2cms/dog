package util;

import java.io.File;

public class FileUtil {

	/*
	 * 在realPath路径下是否存在dir目录,如果存在返回true,如果不存在则创建,并返回是否创建成功
	 * @param dir 目录的物理路径
	 * @return
	 * @author GT 2013-01-28
	 */
	public static boolean mkdirs(String dir){
		File file = new File(dir);
		//如果文件夹不存在则创建    
		if  (!file.exists() && !file.isDirectory())      
		{  
			System.out.println("创建:"+dir);    
		    return file.mkdirs(); //mkdir()创建单个路径  mkdirs()包括父路径
		} else{  
		    return true;
		}
	}
}
