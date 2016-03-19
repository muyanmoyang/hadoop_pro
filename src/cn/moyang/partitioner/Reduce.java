package cn.moyang.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

	public IntWritable IntValue = new IntWritable(0) ;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		int sum = 0 ;
		while(values.iterator().hasNext())
		{
			sum += values.iterator().next().get() ;
		}
		IntValue.set(sum) ;
		
		context.write(key, IntValue) ;
	}
}
