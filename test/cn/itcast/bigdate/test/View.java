package cn.itcast.bigdate.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * @author
 * 
 */
public class View {

	/**
	 * 统计在文件中，这个用户每一次访问的起始请求时间，及该次访问的停留时长
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) {

		AccessLog accessLogs = new AccessLog();
		// [30/May/2013:17:38:25
		SimpleDateFormat sdf = new SimpleDateFormat ("[dd/MM/yyyy:HH:mm:ss ", Locale.ENGLISH);
		BufferedReader br = null;
		try {
			// 构造数据
			br = new BufferedReader(new FileReader("c://data.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] arr = line.split(" - - ");
				Date date = sdf.parse(arr[1]);
				accessLogs.put(date);
			}
			// 分析
			for (List<Date> accessLog : accessLogs.getAccessLogs()) {
				Date startTime = accessLog.get(0);
				Date endTime = accessLog.get(accessLog.size());
				System.out.println("起始时间：" + startTime + "\t" + "访问时长：" + (endTime.getTime() - startTime.getTime()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

/**
 * 一个用户的访问时间段
 * 
 * @author
 * 
 */
class AccessLog {

	private List<List<Date>> accessLogs = new ArrayList<List<Date>>();
	private List<Date> thisAccess = new ArrayList<Date>();
	private long cha = 1 * 60 * 1000;

	public void put(Date date) {
		int size = thisAccess.size();
		if (size == 0) {
			thisAccess.add(date);
			return;
		}

		long lastAccessTime = thisAccess.get(size).getTime();
		long thisAccessTime = date.getTime();
		if (thisAccessTime - lastAccessTime > cha) {// 喝了一会儿茶回来的
			accessLogs.add(thisAccess);
			thisAccess = new ArrayList<Date>();
			thisAccess.add(date);
		} else {// 连续的
			thisAccess.add(date);
		}
	}

	public List<List<Date>> getAccessLogs() {
		return accessLogs;
	}
}
