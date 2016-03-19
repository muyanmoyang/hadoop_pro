package hdfs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class ApplicationTest_01 {
	/*
	public static final String HDFS_PATH = "hdfs://hadoop:9000/muyan" ;
	
	public static void main(String[] args) throws Exception
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory()) ;
		final URL url = new URL(HDFS_PATH) ;
		final InputStream in = url.openStream() ;
		
		IOUtils.copyBytes(in,System.out,1024,true) ;
	}
	*/
	
	static 
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory()) ;
	}	
	
	public static final String HDFS_PATH = "hdfs://hadoop:9000/muyan" ;
	
	public static void main(String[] args) {
		InputStream in = null ;
		try
		{
			final URL url = new URL(HDFS_PATH) ;
			in = url.openStream() ;
			/**
			 * @param in 输入流
			 * @param out 输出流
			 * @param conf 缓冲区大小
			 * @param close 是否关闭流
			 */
			IOUtils.copyBytes(in,System.out,1024,false) ;
		}catch(Exception e)
		{
			e.printStackTrace() ;
		}finally
		{
			IOUtils.closeStream(in) ;
		}
	}
}
