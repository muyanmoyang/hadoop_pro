package cn.moyang.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) throws IOException {
		
		/**
		   * Get a proxy connection to a remote server
		   * 获得一个远程服务端的代理对象，该对象实现了命名的协议，代理对象会与指定的服务端通话。
		   * @param protocol protocol class  
		   * @param clientVersion client version
		   * @param addr remote address
		   * @param conf configuration to use
		   * @param connTimeout time in milliseconds before giving up
		   * @return the proxy
		   * @throws IOException if the far end through a RemoteException
		   */
		Mybizable proxy = (Mybizable) RPC.waitForProxy(
					Mybizable.class,
					Mybizable.VERSION,
					new InetSocketAddress(MyServer.ADDRESS,MyServer.PORT),
					new Configuration()
			      	) ;
			
		System.out.println("客户端结果："+proxy.Hello(" muyanmoyang")) ;
		RPC.stopProxy(proxy) ;
	}
		
}
