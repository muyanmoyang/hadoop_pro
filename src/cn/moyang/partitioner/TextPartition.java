package cn.moyang.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public  class TextPartition extends Partitioner<Text,Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		
		int result = 0 ;
		
		if(key.equals("short"))
		{
			result = 0 % numPartitions ;
		}else if(key.equals("long"))
		{
			result = 1 % numPartitions ;
		}else if(key.equals("right"))
		{
			result = 2 % numPartitions ;
		}
		return result ; 
	}
}
