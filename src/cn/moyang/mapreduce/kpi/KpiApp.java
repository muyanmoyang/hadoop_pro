package cn.moyang.mapreduce.kpi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 使用自定义的Hadoop类型案例
 * @author hadoop
 *
 */

public class KpiApp {
	
	static final String INPUT_PATH = "hdfs://hadoop:9000/in" ;
	static final String OUTPUT_PATH = "hdfs://hadoop:9000/out" ;
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration() ;
		Job job = new Job(conf,KpiApp.class.getSimpleName()) ;
		
		FileInputFormat.setInputPaths(job, INPUT_PATH) ;
	
		job.setInputFormatClass(TextInputFormat.class) ;
		
		job.setMapperClass(MyMapper.class) ;
		job.setMapOutputKeyClass(Text.class) ;
		job.setMapOutputValueClass(KpiWritable.class) ;
		
		job.setPartitionerClass(HashPartitioner.class) ;
		job.setNumReduceTasks(1) ;
		
		job.setOutputValueClass(MyReducer.class) ;
		job.setOutputKeyClass(Text.class) ;
		job.setOutputValueClass(KpiWritable.class) ;
		
		FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH)) ;
		job.setOutputFormatClass(TextOutputFormat.class) ;
		
		System.exit(job.waitForCompletion(true)? 0 : 1) ;
	}
	
	static class MyMapper extends Mapper<LongWritable,Text,Text,KpiWritable>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			final String[] splited = value.toString().split("\t");
			final String msisdn = splited[1] ;
			final Text k2 = new Text(msisdn) ;
			final KpiWritable v2 = new KpiWritable(splited[6],splited[7],splited[8],splited[9]) ;
			context.write(k2,v2) ;
		}		
	}
	
	static class MyReducer extends Reducer<Text,KpiWritable,Text,KpiWritable>
	{

		protected void reduce(Text k2, Iterable<KpiWritable> v2,org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			for(KpiWritable kpiWritable : v2)
			{
				upPackNum += kpiWritable.upPackNum ;
				downPackNum += kpiWritable.downPackNum ;
				upPayLoad += kpiWritable.upPayLoad ;
				downPayLoad += kpiWritable.downPayLoad ;
			}
			final KpiWritable v3 = new KpiWritable(upPackNum+"",downPackNum+"",upPayLoad+"",downPayLoad+"") ;
			context.write(k2,v3) ;
		}
	}
}

class KpiWritable implements Writable
{
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;
	
	public KpiWritable(){}
	public KpiWritable(String upPackNum,String downPackNum,String upPayLoad,String downPayLoad)
	{
		this.upPackNum = Long.parseLong(upPackNum) ;
		this.downPackNum = Long.parseLong(downPackNum) ;
		this.upPayLoad = Long.parseLong(upPayLoad) ;
		this.downPayLoad = Long.parseLong(downPayLoad) ;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong() ;
		this.downPackNum = in.readLong() ;
		this.upPayLoad = in.readLong() ;
		this.downPayLoad = in.readLong() ;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum) ;
		out.writeLong(downPackNum) ;
		out.writeLong(upPayLoad) ;
		out.writeLong(downPayLoad) ;
	}
	@Override
	public String toString() {
		return upPackNum +"\t" + downPackNum +"\t" + upPayLoad +"\t" + downPayLoad;
	}
}
