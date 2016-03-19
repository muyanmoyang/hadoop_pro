package cn.moyang.RPC;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface Mybizable extends VersionedProtocol{
	
	Long VERSION = 12345612L ;
	
	public abstract String Hello(String name) ;
	
}