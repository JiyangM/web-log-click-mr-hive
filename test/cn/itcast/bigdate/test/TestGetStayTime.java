package cn.itcast.bigdate.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.Test;

public class TestGetStayTime {

	@Test
	public void test() throws Exception {
		String str1 = "28/Sep/2013:15:39:20";
		String str2 = "19/Sep/2013:01:57:46";
		String str3 = "19/Sep/2013:01:49:46";
		String str4 = "19/Sep/2013:02:57:46";
		String str5 = "19/Sep/2013:02:49:46";
		String str6 = "19/Sep/2013:02:58:46";
		String str7 = "19/Sep/2013:03:49:46";
		String str8 = "19/Sep/2013:03:57:46";
		System.out.println(str1.compareTo(str2));
		System.out.println(str2.compareTo(str3));

		ArrayList<Date> times = new ArrayList<Date>();
		times.add(toDate(str1));
		times.add(toDate(str2));
		times.add(toDate(str3));
		times.add(toDate(str4));
		times.add(toDate(str5));
		times.add(toDate(str6));
		times.add(toDate(str7));
		times.add(toDate(str8));

		Collections.sort(times);
/*		for (Date str : strs) {
			System.out.println(str);
		}

		long span = timeDiff(toDate(str2), toDate(str3));
		System.out.println(span > 30 * 60 * 1000);
*/
		HashMap<Date, Long> stayTime = getStayTime(times);
 

		Set<Entry<Date, Long>> entrySet = stayTime.entrySet();
		for (Entry<Date, Long> ent : entrySet) {

			System.out.println(ent.getKey() + "\t" + ent.getValue());
		}

	}

	private Date toDate(String timeStr) throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyy/MMM/dd:HH:mm:ss", Locale.US);
		return df.parse(timeStr);
	}

	private long timeDiff(Date time1, Date time2) throws ParseException {

		return time1.getTime() - time2.getTime();

	}

	private HashMap<Date, Long> getStayTime(List<Date> times) throws ParseException {
		HashMap<Date, Long> stayTime = new HashMap<Date, Long>();
		int size = times.size();
		Date first = times.get(0);
		stayTime.put(first, 0L);
		for (int i = 0; i < size - 1; i++) {
			for (int j = i + 1; j < size; j++) {
				long timeSpan = timeDiff(times.get(j), times.get(i));
				if (timeSpan > 30 * 60 * 1000) {
					// 如果临近两次时间差超过30分钟，则将"上次-first"的时间差存入hashmap完成一次访问停留处理
					stayTime.put(times.get(i), timeDiff(times.get(j - 1), times.get(i)));
					// 同时，重置first为本次
					stayTime.put(times.get(j), 0L);
					i = j - 1;
					break;
					// 如果一直是连续请求到最后一个，则将 "本次-first"的时间差存入hashmap完成最后一次访问停留处理
				} else {
					if (j == size - 1) {
						stayTime.put(times.get(i), timeDiff(times.get(j), times.get(i)));
					}
				}
			}
		}
		return stayTime;
	}
}
