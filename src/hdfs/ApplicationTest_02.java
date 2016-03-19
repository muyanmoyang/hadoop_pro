package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
/**
 * 使用FileSystem类对HDFS进行读取和写入操作
 * 
 * 创建方法快捷键 	Alt + Shift + M
 * 创建局部变量快捷键		 Alt + Shift + L
 * @author acer
 *
 */
public class ApplicationTest_02 {
	
	public static final String HDFS_PATH = "hdfs://hadoop:9000" ;
	public static final String DIR_PATH = "/MuyanHadoopFileDir" ;
	public static final String FILE_PATH = "/MuyanHadoopFileDir/MuyanHadoopFile" ;
	
	
	public static void main(String[] args) throws Exception {
		
		final FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH),new Configuration()) ;
			
		//创建文件夹
//		makeDirectory(fileSystem);
		//上传文件
//		uploadFile(fileSystem);
		//下载文件
//		downloadFile(fileSystem);
		//删除文件(夹)
//		removeFile(fileSystem);
		
		//列出文件或目录的信息 listStatus()方法
		Path path = new Path(DIR_PATH) ;
		FileStatus[] fileStatus = fileSystem.listStatus(path) ;
		for(int i=0 ; i<fileStatus.length ;i++)
		{
			System.out.println(fileStatus[i]);
		}
		
		//读取文件或目录的元数据信息，如：文件长度、块大小、备份、修改时间、所有者、权限信息等
		Path file = new Path(FILE_PATH) ;
		FileStatus stat = fileSystem.getFileStatus(file) ;
		String group = stat.getGroup() ;
		Path filePath = stat.getPath() ;
		long accessTime = stat.getAccessTime() ;
		long blockSize = stat.getBlockSize() ;
		long length = stat.getLen() ;
		
		String modifyTime = new Timestamp(stat.getModificationTime()).toString() ;
		/**
		 * Timestamp类，一个与 java.util.Date 类有关的瘦包装器 (thin wrapper)，它允许 JDBC API 将该类标识为 SQL TIMESTAMP值。
		 * 它通过允许小数秒到纳秒级精度的规范来添加保存 SQL TIMESTAMP 小数秒值的能力。Timestamp 也提供支持
		 * 时间戳值的 JDBC 转义语法的格式化和解析操作的能力。 
		 */
		/*错误的格式调整
		String stringDate = Long.toString(modifyTime) ;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") ;
		Date modifyDate = dateFormat.parse(stringDate) ;
		*/
		FsPermission  permission = stat.getPermission() ;//文件权限信息
		
		String owner = stat.getOwner() ;  //文件所有者
		boolean isDir = stat.isDir() ; //是否为目录
		String isDirectory ;
		if(isDir == true)
			isDirectory = "是" ;
		else
			isDirectory = "不是" ;
		
		System.out.println
		(group+"  ,filePath:"+filePath+"   ,accessTime:"+accessTime+"   ,blockSize:"+blockSize/1024/1024+"M");
		System.out.println("length:"+length+"   ,modifyTime:"+modifyTime);
		System.out.println("权限信息:"+permission+"   ，所有者:"+owner);
		System.out.println("是否为目录："+isDirectory) ;
	}

	private static void removeFile(final FileSystem fileSystem)
			throws IOException {
		fileSystem.delete(new Path(FILE_PATH),true) ;
	}

	private static void downloadFile(final FileSystem fileSystem)
			throws IOException { 
		final FSDataInputStream in = fileSystem.open(new Path(FILE_PATH)) ;
		IOUtils.copyBytes(in, System.out, 1024, true) ;
	}

	private static void uploadFile(final FileSystem fileSystem)
			throws IOException, FileNotFoundException {
		FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH)) ;
		
		final FileInputStream in = new FileInputStream("E:/Desktop/好东西/毕设.txt") ;
		
		IOUtils.copyBytes(in, out, 1024,true) ;
	}

	private static void makeDirectory(final FileSystem fileSystem)
			throws IOException {
		fileSystem.mkdirs(new Path(DIR_PATH));
	}
}
