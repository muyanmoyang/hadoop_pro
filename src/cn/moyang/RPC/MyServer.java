package cn.moyang.RPC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {
	
	static final String ADDRESS = "localhost" ;
	static final int PORT = 12345 ;
	
	public static void main(String[] args) throws IOException {
		
		/** Construct an RPC server. 构造一个RPC服务端
	     * @param instance 这个方法中的方法会被调用
	     * @param conf the configuration to use
	     * @param bindAddress 绑定的地址是用于监听连接的
	     * @param port 绑定的端口是用于监听连接的
	     */
		final Server server = RPC.getServer(new MyBiz(), ADDRESS, PORT,new Configuration()) ;
		server.start() ;
	}
}
