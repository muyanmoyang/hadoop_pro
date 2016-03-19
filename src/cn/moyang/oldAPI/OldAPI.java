package cn.moyang.oldAPI;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.TestMiniMRClientCluster.MyReducer;
import org.apache.hadoop.mapred.lib.HashPartitioner;

/**
 * 旧版API
 * @author hadoop
 *
 */

public class OldAPI {
	/**
	 * 改动：
	 * 1.不再使用Job，而是使用JobConf
	 * 2.类的包名不再使用mapreduce，而是使用mapred
	 * 3.不再使用job.waitForCompletion(true)提交作业，而是使用JobClient.runJob(job);
	 * 
	 */
	static final String INPUT_PATH = "hdfs://hadoop:9000/hello";
	static final String OUT_PATH = "hdfs://hadoop:9000/out";
	public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration() ;
			JobConf job = new JobConf(conf) ;
			
			FileInputFormat.setInputPaths(job, INPUT_PATH) ;
		
			job.setInputFormat(TextInputFormat.class) ;
			
			job.setMapperClass(MyMapper.class) ;
			job.setMapOutputKeyClass(Text.class) ;
			job.setMapOutputValueClass(OldAPI.class) ;
			
			job.setPartitionerClass(HashPartitioner.class) ;
			job.setNumReduceTasks(1) ;
			
			job.setOutputValueClass(MyReducer.class) ;
			job.setOutputKeyClass(Text.class) ;
			job.setOutputValueClass(OldAPI.class) ;
			
			FileOutputFormat.setOutputPath(job,new Path(OUT_PATH)) ;
			job.setOutputFormat(TextOutputFormat.class) ;
			JobClient.runJob(job) ;
	}	

static class MyMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,LongWritable>{

		public void map(LongWritable key, Text value,OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			
			final String[] splited = value.toString().split("\t");
			for(String word : splited)
			{
				output.collect(new Text(word),new LongWritable(1L)) ;
			}
		}
	}
	
static class MyReuce extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>
{
		public void reduce(Text key, Iterator<LongWritable> values,OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			
			long sum = 0L ;
			while(values.hasNext())
			{
				final long temp = values.next().get() ;
				sum += temp ;
			}
			output.collect(key,new LongWritable(sum)) ;
		}
	}
}