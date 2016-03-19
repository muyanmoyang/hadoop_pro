package cn.moyang.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SortApp {

	static final String INPUT_PATH = "hdfs://hadoop:9000/in" ; 
	static final String OUT_PATH = "hdfs://hadoop:9000/out" ; 
	
	public static void main(String[] args) throws Exception {
		
		final Configuration configuration = new Configuration() ;
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),configuration) ;
		if(fileSystem.exists(new Path(OUT_PATH)))
		{
			fileSystem.delete(new Path(OUT_PATH),true) ;	
		}
		
		final Job job  = new Job(configuration,SortApp.class.getSimpleName()) ;
		//1.1 ָ�������ļ�·��
		FileInputFormat.setInputPaths(job,INPUT_PATH) ;
		//ָ���ĸ���������ʽ�������ļ�
		job.setInputFormatClass(TextInputFormat.class) ; 
		//1.2ָ���Զ����Mapper��
		job.setMapperClass(MyMapper.class) ;
		
		//ָ�����<k2,v2>������
		job.setMapOutputKeyClass(NewK2.class) ;
		job.setMapOutputValueClass(LongWritable.class) ;
		
		//1.3 ָ��������
		job.setPartitionerClass(HashPartitioner.class) ;
		job.setNumReduceTasks(1) ;
		
		//1.4 ���򡢷���
		job.setGroupingComparatorClass(MyGroupingComparator.class) ;
		
		//1.5  TODO ����ѡ���ϲ�
		
		//2.2 ָ���Զ����reduce��
		job.setReducerClass(MyReducer.class) ;
		
		//ָ�����<k3,v3>������
		job.setOutputKeyClass(LongWritable.class) ;
		job.setOutputValueClass(LongWritable.class) ;
		
		//2.3 ָ�����������
		FileOutputFormat.setOutputPath(job,new Path(OUT_PATH)) ;
		//�趨����ļ��ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//�Ѵ����ύ��JobTrackerִ��
		System.exit(job.waitForCompletion(true) ? 0 : 1) ;
	}
	
	static class MyMapper extends Mapper<LongWritable,Text,NewK2,LongWritable>
	{
		protected void map(LongWritable key, Text value, Context context) 
			throws IOException ,InterruptedException {
			
			final String[] splited = value.toString().split("\t");
			
			final NewK2 k2 = new NewK2(Long.parseLong(splited[0]),Long.parseLong(splited[1]));
			LongWritable v2 = new LongWritable(Long.parseLong(splited[1])) ;
			context.write(k2,v2) ;
		};
	}
	static class MyReducer extends Reducer<NewK2,LongWritable,LongWritable,LongWritable>
	{
		protected void reduce(NewK2 k2, java.lang.Iterable<LongWritable> v2s, Context context) 
				throws IOException ,InterruptedException {
			
			long min = Long.MAX_VALUE ;
			for(LongWritable v2 : v2s)
			{
				if(v2.get()<min)
				{
					min = v2.get() ;
				}
			}
			context.write(new LongWritable(k2.first),new LongWritable(min)) ;
		};
	}
	
	static class NewK2 implements WritableComparable<NewK2>
	{
		Long first;
		Long second;

		public NewK2(){}
		public NewK2(long first,long second)
		{
			this.first = first ;
			this.second = second ;
		}
		
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong() ;
			this.second = in.readLong() ;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(first) ;
			out.writeLong(second) ;
		}

		public int compareTo(NewK2 o) {
			final long minus = this.first - o.first ;
			if(minus != 0)
			{
				return (int) minus ;
			}
			return (int) (this.second - o.second);
		}

		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof NewK2))
			{
				return false ;
			}
			NewK2 oK2 = (NewK2) obj ;
			return (this.first == oK2.first && this.second == oK2.second) ;
		}
		@Override
		public int hashCode() {
			return this.first.hashCode() + this.second.hashCode() ;
		}
	}
	/**
	 * �ʣ�Ϊʲô�Զ�����ࣿ
	 * ��ҵ��Ҫ������ǰ��յ�һ�з��飬����NewK2�ıȽϹ�������˲��ܰ��յ�һ�з֡�ֻ���Զ������Ƚ�����
	 */
	static class MyGroupingComparator implements RawComparator<NewK2>
	{

		/**
		 * @param arg0 ��ʾ��һ������Ƚϵ��ֽ�����
		 * @param arg1 ��ʾ��һ������Ƚϵ��ֽ��������ʼλ��
		 * @param arg2 ��ʾ��һ������Ƚϵ��ֽ������ƫ����
		 * 
		 * @param arg3 ��ʾ�ڶ�������Ƚϵ��ֽ�����
		 * @param arg4 ��ʾ�ڶ�������Ƚϵ��ֽ��������ʼλ��
		 * @param arg5 ��ʾ�ڶ�������Ƚϵ��ֽ������ƫ����
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
		}

		public int compare(NewK2 o1, NewK2 o2) {
			return (int) (o1.first - o2.first) ;
		}
	}
}


