package cn.moyang.algorithm;

/**
 * TopK�㷨
 */
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.moyang.partitioner.Reduce;

public class TopKApp {
	
	final static String INPUT_PATH = "hdfs://hadoop:9000/in" ;
	final static String OUTPUT_PATH = "hdfs://hadoop:9000/out" ;
	
	public static void main(String[] args) throws Exception {
		
		final Configuration configuration = new Configuration() ;
		
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),configuration) ;
		if(fileSystem.exists(new Path(OUTPUT_PATH)))
		{
			fileSystem.delete(new Path(OUTPUT_PATH),true);
		}
		Job job = new Job(configuration,TopKApp.class.getSimpleName()) ;
		
		FileInputFormat.setInputPaths(job,INPUT_PATH) ;
		job.setInputFormatClass(TextInputFormat.class) ;
		job.setMapperClass(MyMapper.class) ;
		job.setMapOutputKeyClass(LongWritable.class) ;
		job.setMapOutputValueClass(NullWritable.class) ;
		
		job.setOutputFormatClass(TextOutputFormat.class) ;
		job.setReducerClass(MyReduce.class) ;
		job.setOutputKeyClass(LongWritable.class) ;
		job.setOutputValueClass(NullWritable.class) ;
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH)) ;

		System.exit(job.waitForCompletion(true)? 0 : 1) ;
	}
	
	static class MyMapper extends Mapper<LongWritable,Text,LongWritable,NullWritable>
	{
		
		long max = Long.MIN_VALUE ;
		protected void map(LongWritable k1,Text v1,Context context)
			throws java.io.IOException ,InterruptedException {
			
			final long temp = Long.parseLong(v1.toString());
			if(temp > max)
			{
				max = temp ;
			}
		};
		protected void cleanup(Context context) throws java.io.IOException ,InterruptedException {
			
			context.write(new LongWritable(max),NullWritable.get()) ;
		};
	}
	
	static class MyReduce extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable>
	{
		long max = Long.MIN_VALUE ;
		protected void reduce(LongWritable k2, java.lang.Iterable<NullWritable> v2, Context context) 
				throws java.io.IOException ,InterruptedException {
			
			Long temp = k2.get() ;
			if(temp > max)
			{
				max = temp ;
			}
		};
		
		protected void cleanup(Context context) 
			throws java.io.IOException ,InterruptedException {
			context.write(new LongWritable(max),NullWritable.get()) ;
		};
	}
}
