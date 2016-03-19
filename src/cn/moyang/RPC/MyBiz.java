package cn.moyang.RPC;

public class MyBiz implements Mybizable {
	
	public String Hello(String name)
	{
		System.out.println("***************");
		return "hello" + name ; 
	}
	
	public long getProtocolVersion(String args0,long args1)
	{
		return VERSION ; 
	}
}
