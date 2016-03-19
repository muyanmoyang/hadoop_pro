package cn.moyang.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) throws IOException {
		
		/**
		   * Get a proxy connection to a remote server
		   * ���һ��Զ�̷���˵Ĵ�����󣬸ö���ʵ����������Э�飬����������ָ���ķ����ͨ����
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
			
		System.out.println("�ͻ��˽����"+proxy.Hello(" muyanmoyang")) ;
		RPC.stopProxy(proxy) ;
	}
		
}
