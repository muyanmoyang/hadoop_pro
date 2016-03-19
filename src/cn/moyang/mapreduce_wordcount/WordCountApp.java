package cn.moyang.mapreduce_wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountApp {
	
	static final String INPUT_PATH = "hdfs://hadoop:9000/in" ;
	static final String OUTPUT_PATH = "hdfs://hadoop:9000/out" ;
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration() ;
		final Job job = new Job(configuration,WordCountApp.class.getSimpleName()) ;
		
		//1.1指定读取的文件位于哪里
		FileInputFormat.setInputPaths(job,INPUT_PATH) ;
		
		//指定如何对输入文件进行格式化，把输入文件每一行解析成键值对
		job.setInputFormatClass(TextInputFormat.class) ;	
		
		//1.2 指定自定义的map类
		job.setMapperClass(MyMap.class) ;
		//map输出的<k,v>类型。如果<k3,v3>的类型与<k2,v2>类型一致，则可以省略
		job.setMapOutputKeyClass(Text.class) ;
		job.setMapOutputValueClass(LongWritable.class) ;
		//1.3 分区
		job.setPartitionerClass(HashPartitioner.class) ;
		//有一个reduce任务运行
		job.setNumReduceTasks(1) ;
		
		//1.4 TODO 排序、分组
		//1.5 TODO 规约
		//2.2 指定自定义reduce类
		job.setReducerClass(MyReduce.class) ;
		//指定reduce的输出类型
		job.setOutputKeyClass(Text.class) ;
		job.setOutputValueClass(LongWritable.class) ;
		
		//2.3 指定写出到哪里
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH)) ;
		//指定输出文件的格式化类
		job.setOutputFormatClass(TextOutputFormat.class) ;
		//把job提交给JobTracker运行
		System.exit(job.waitForCompletion(true)? 0 : 1) ;
	}
	
	static class MyMap extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			/**
			 * 自定义计数器helloCounter
			 */
			final String stringLine = value.toString();
			final Counter helloCounter = context.getCounter("sensitive words","200") ;
			if(stringLine.contains("200"))
			{
				helloCounter.increment(1L) ;
			}
			final String[] splited = stringLine.split("\t");
			for(String word : splited)
			{
				context.write(new Text(word),new LongWritable(1L)) ;
			}
		}
	}
	
	static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		protected void reduce(Text key, Iterable<LongWritable> value,Context context)
				throws IOException, InterruptedException {
			
			long sum = 0L ; 
			for(LongWritable v2 : value)
			{
				sum += v2.get() ;
			}
			context.write(key,new LongWritable(sum)) ;
		}
	}
}
