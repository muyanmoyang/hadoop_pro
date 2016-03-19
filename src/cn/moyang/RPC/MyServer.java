package cn.moyang.RPC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {
	
	static final String ADDRESS = "localhost" ;
	static final int PORT = 12345 ;
	
	public static void main(String[] args) throws IOException {
		
		/** Construct an RPC server. ����һ��RPC�����
	     * @param instance ��������еķ����ᱻ����
	     * @param conf the configuration to use
	     * @param bindAddress �󶨵ĵ�ַ�����ڼ������ӵ�
	     * @param port �󶨵Ķ˿������ڼ������ӵ�
	     */
		final Server server = RPC.getServer(new MyBiz(), ADDRESS, PORT,new Configuration()) ;
		server.start() ;
	}
}
