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
		
		//1.1ָ����ȡ���ļ�λ������
		FileInputFormat.setInputPaths(job,INPUT_PATH) ;
		
		//ָ����ζ������ļ����и�ʽ�����������ļ�ÿһ�н����ɼ�ֵ��
		job.setInputFormatClass(TextInputFormat.class) ;	
		
		//1.2 ָ���Զ����map��
		job.setMapperClass(MyMap.class) ;
		//map�����<k,v>���͡����<k3,v3>��������<k2,v2>����һ�£������ʡ��
		job.setMapOutputKeyClass(Text.class) ;
		job.setMapOutputValueClass(LongWritable.class) ;
		//1.3 ����
		job.setPartitionerClass(HashPartitioner.class) ;
		//��һ��reduce��������
		job.setNumReduceTasks(1) ;
		
		//1.4 TODO ���򡢷���
		//1.5 TODO ��Լ
		//2.2 ָ���Զ���reduce��
		job.setReducerClass(MyReduce.class) ;
		//ָ��reduce���������
		job.setOutputKeyClass(Text.class) ;
		job.setOutputValueClass(LongWritable.class) ;
		
		//2.3 ָ��д��������
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH)) ;
		//ָ������ļ��ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class) ;
		//��job�ύ��JobTracker����
		System.exit(job.waitForCompletion(true)? 0 : 1) ;
	}
	
	static class MyMap extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			/**
			 * �Զ��������helloCounter
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
