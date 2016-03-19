package cn.moyang.mapreduce.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import cn.moyang.partitioner.Reduce;

public class Step1 {	
	static class Step1Mapper extends Mapper<Object, Text, IntWritable, Text> {
		protected void map(Object key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			String tokens[] = Recommend.DELIMITER.split(value.toString());
			int userId = Integer.parseInt(tokens[0]);
			String itemId = tokens[1];
			String pref = tokens[2];
			context.write(new IntWritable(userId),
					new Text(itemId + ":" + pref));
		}
	}

	static class Step1Reducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		protected void reduce(IntWritable key, java.lang.Iterable<Text> value,
				Context context) throws java.io.IOException,
				InterruptedException {
			StringBuilder sb = new StringBuilder();
			while (value.iterator().hasNext()) {
				sb.append("," + value.iterator().next());
			}
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
		}
	}

	public static void run(Map<String, String> path) throws IOException {
		JobConf conf = Recommend.config();
		String input = path.get("Step1Input");
		String output = path.get("Step1Output");
		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		// hdfs.rmr(output);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(path.get("data"), input);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) Step1Mapper.class);
		conf.setCombinerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) Step1Reducer.class);
		conf.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) Step1Reducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			job.waitForCompletion();
		}
	}
}