package cn.itcast.bigdata.weblog.old;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itcast.bigdata.weblog.mrbean.WebLogBean;
import cn.itcast.bigdata.weblog.pre.WebLogParser;

public class UserStayTime {

	static class UserStayTimeMapper extends Mapper<LongWritable, Text, Text, Text> {

		Text k = new Text();
		Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Counter invalidCounter = context.getCounter("parsetime", "invalidbean");
			String line = value.toString();

			WebLogBean bean = WebLogParser.parser(line);
			if (bean.isValid()) {
				String remote_addr = bean.getRemote_addr();
				String time_local = bean.getTime_local();

				k.set(remote_addr);
				v.set(time_local);

				context.write(k, v);
			} else {
				invalidCounter.increment(1);
			}
		}
	}

	static class UserStayTimeReducer extends Reducer<Text, Text, Text, Text> {

		Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Counter staytimeerr = context.getCounter("parsetime", "staytimeerr");
			ArrayList<Date> times = new ArrayList<Date>();
			try {
				for (Text value : values) {
					times.add(toDate(value.toString()));
				}

				Collections.sort(times);
				HashMap<Date[], Long> stayTime = getStayTime(times);

				Set<Entry<Date[], Long>> entrySet = stayTime.entrySet();
				for (Entry<Date[], Long> ent : entrySet) {
					// 输出 remote_addr local_time timeSpan
					v.set(toStr(ent.getKey()[0]) + "\t" +toStr(ent.getKey()[1]) + "\t" + ent.getValue());
					context.write(key, v);

				}

			} catch (ParseException e) {
				staytimeerr.increment(1);

			}

		}

		private String toStr(Date date) {
			SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
			return df.format(date);
		}

		private Date toDate(String timeStr) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
			return df.parse(timeStr);
		}

		/**
		 * 获取连续请求中的每次访问及停留时长
		 * 
		 * @param times
		 * @return
		 * @throws ParseException
		 */
		private HashMap<Date[], Long> getStayTime(ArrayList<Date> times) throws ParseException {

			HashMap<Date[], Long> stayTime = new HashMap<Date[], Long>();
			int size = times.size();
			Date[] dates = new Date[2];
			dates[0] = times.get(0);
			//如果times元素个数只有1个，则是该用户单请求，则直接返回
			if(size<1) return null;
			if(size<2){
				dates[1] = dates[0];
				stayTime.put(dates, 0L);
				return stayTime;
			}
			for (int i = 0; i < size-1; i++) {
				if (timeDiff(times.get(i+1), times.get(i)) > 30*60*1000) {
					// 如果"下次-本次"之间时间差超过30分钟，则将"本次-批首"的时间差存入hashmap完成一次访问停留处理
					dates[1] = times.get(i);
					stayTime.put(dates, timeDiff(dates[1],dates[0]));
					// 同时，重置批次
					dates = new Date[2];
					dates[0] = times.get(i + 1);
				} else {
					// 如果一直是连续请求到最后一个，则将 "下次-批首"的时间差存入hashmap完成最后一次访问停留处理
					if (i == size-2) {
						dates[1] = times.get(i+1);
						stayTime.put(dates, timeDiff(dates[1], dates[0]));
					}
				}

			}

			return stayTime;
		}

		private long timeDiff(String time1, String time2) throws ParseException {

			Date d1 = toDate(time1);
			Date d2 = toDate(time2);
			return d1.getTime() - d2.getTime();

		}

		private long timeDiff(Date time1, Date time2) throws ParseException {

			return time1.getTime() - time2.getTime();

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(UserStayTime.class);

		job.setMapperClass(UserStayTimeMapper.class);
		job.setReducerClass(UserStayTimeReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));*/

		
		 FileInputFormat.setInputPaths(job, new Path("c:/weblog/input"));
		 FileOutputFormat.setOutputPath(job, new Path("c:/weblog/output"));
		 

		job.waitForCompletion(true);

	}

}
