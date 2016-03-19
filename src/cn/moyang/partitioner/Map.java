package cn.moyang.partitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object,Text,Text,IntWritable>{

	private Text keyText = new Text("key") ;
	private IntWritable intValue = new IntWritable(1) ;
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//get data
		String str = value.toString() ;
		//split
		StringTokenizer stringTokenizer = new StringTokenizer(str) ;
		//itearator
		while(stringTokenizer.hasMoreTokens())
		{
			keyText.set(stringTokenizer.nextToken()) ;
			context.write(keyText,intValue) ;
		}
		
	}
}
