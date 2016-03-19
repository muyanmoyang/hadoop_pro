package cn.moyang.hadoop.KPI;

import java.net.URI;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPITime {
	
	final static String INPUT_PATH = "hdfs://hadoop1:9000/user/root/testdata/access.20120104.log" ;
	final static String OUTPUT_PATH = "hdfs://hadoop1:9000/user/root/12-18-output" ;
	
	public static void main(String[] args) throws Exception{
		final Configuration configuration = new Configuration() ;
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),configuration) ;
		if(fileSystem.exists(new Path(OUTPUT_PATH)))
		{
			fileSystem.delete(new Path(OUTPUT_PATH),true) ;
		}
		Job job = new Job(configuration,KPIPV.class.getSimpleName()) ;
		
		FileInputFormat.setInputPaths(job,INPUT_PATH) ;
		job.setInputFormatClass(TextInputFormat.class) ;
		job.setMapperClass(KPITimeMapper.class) ;
		job.setMapOutputKeyClass(Text.class) ;
		job.setMapOutputValueClass(IntWritable.class) ;
		
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH)) ;
		job.setReducerClass(KPITimeReducer.class) ;
		job.setOutputFormatClass(TextOutputFormat.class) ;
		job.setOutputKeyClass(Text.class) ;
		job.setOutputValueClass(IntWritable.class) ;
		
		System.exit(job.waitForCompletion(true)? 0 : 1) ;
	}	
	
	static class KPITimeMapper extends Mapper<Object,Text, Text,IntWritable>
	{
		protected void map(Object key1, Text value1, Context context) 
					throws java.io.IOException ,InterruptedException 
		{
			KPI kpi = KPI.filterTime(value1.toString());
			if(kpi.isValid())
			{				
				try {
					context.write(new Text(kpi.getTime_local_Date_hour()),new IntWritable(1)) ;
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}
	static class KPITimeReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable() ;
		protected void reduce(Text key1, java.lang.Iterable<IntWritable> value1, Context context) 
					throws java.io.IOException ,InterruptedException 
		{
			int sum = 0 ;
			while(value1.iterator().hasNext())
			{
				sum += value1.iterator().next().get();
			}
			result.set(sum) ;
			context.write(key1,result) ;
		}
	}
}
