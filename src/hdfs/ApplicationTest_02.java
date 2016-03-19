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
 * ʹ��FileSystem���HDFS���ж�ȡ��д�����
 * 
 * ����������ݼ� 	Alt + Shift + M
 * �����ֲ�������ݼ�		 Alt + Shift + L
 * @author acer
 *
 */
public class ApplicationTest_02 {
	
	public static final String HDFS_PATH = "hdfs://hadoop:9000" ;
	public static final String DIR_PATH = "/MuyanHadoopFileDir" ;
	public static final String FILE_PATH = "/MuyanHadoopFileDir/MuyanHadoopFile" ;
	
	
	public static void main(String[] args) throws Exception {
		
		final FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH),new Configuration()) ;
			
		//�����ļ���
//		makeDirectory(fileSystem);
		//�ϴ��ļ�
//		uploadFile(fileSystem);
		//�����ļ�
//		downloadFile(fileSystem);
		//ɾ���ļ�(��)
//		removeFile(fileSystem);
		
		//�г��ļ���Ŀ¼����Ϣ listStatus()����
		Path path = new Path(DIR_PATH) ;
		FileStatus[] fileStatus = fileSystem.listStatus(path) ;
		for(int i=0 ; i<fileStatus.length ;i++)
		{
			System.out.println(fileStatus[i]);
		}
		
		//��ȡ�ļ���Ŀ¼��Ԫ������Ϣ���磺�ļ����ȡ����С�����ݡ��޸�ʱ�䡢�����ߡ�Ȩ����Ϣ��
		Path file = new Path(FILE_PATH) ;
		FileStatus stat = fileSystem.getFileStatus(file) ;
		String group = stat.getGroup() ;
		Path filePath = stat.getPath() ;
		long accessTime = stat.getAccessTime() ;
		long blockSize = stat.getBlockSize() ;
		long length = stat.getLen() ;
		
		String modifyTime = new Timestamp(stat.getModificationTime()).toString() ;
		/**
		 * Timestamp�࣬һ���� java.util.Date ���йص��ݰ�װ�� (thin wrapper)�������� JDBC API �������ʶΪ SQL TIMESTAMPֵ��
		 * ��ͨ������С���뵽���뼶���ȵĹ淶����ӱ��� SQL TIMESTAMP С����ֵ��������Timestamp Ҳ�ṩ֧��
		 * ʱ���ֵ�� JDBC ת���﷨�ĸ�ʽ���ͽ��������������� 
		 */
		/*����ĸ�ʽ����
		String stringDate = Long.toString(modifyTime) ;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") ;
		Date modifyDate = dateFormat.parse(stringDate) ;
		*/
		FsPermission  permission = stat.getPermission() ;//�ļ�Ȩ����Ϣ
		
		String owner = stat.getOwner() ;  //�ļ�������
		boolean isDir = stat.isDir() ; //�Ƿ�ΪĿ¼
		String isDirectory ;
		if(isDir == true)
			isDirectory = "��" ;
		else
			isDirectory = "����" ;
		
		System.out.println
		(group+"  ,filePath:"+filePath+"   ,accessTime:"+accessTime+"   ,blockSize:"+blockSize/1024/1024+"M");
		System.out.println("length:"+length+"   ,modifyTime:"+modifyTime);
		System.out.println("Ȩ����Ϣ:"+permission+"   ��������:"+owner);
		System.out.println("�Ƿ�ΪĿ¼��"+isDirectory) ;
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
		
		final FileInputStream in = new FileInputStream("E:/Desktop/�ö���/����.txt") ;
		
		IOUtils.copyBytes(in, out, 1024,true) ;
	}

	private static void makeDirectory(final FileSystem fileSystem)
			throws IOException {
		fileSystem.mkdirs(new Path(DIR_PATH));
	}
}
