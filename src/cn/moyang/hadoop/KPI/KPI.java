package cn.moyang.hadoop.KPI;

import java.net.HttpRetryException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class KPI {
	private String remote_addr;// ��¼�ͻ��˵�ip��ַ
	private String remote_user;// ��¼�ͻ����û�����,��������"-"
	private String time_local;// ��¼����ʱ����ʱ��
	private String request;// ��¼�����url��httpЭ��
	private String status;// ��¼����״̬���ɹ���200
	private String body_bytes_sent;// ��¼���͸��ͻ����ļ��������ݴ�С
	private String http_referer;// ������¼���Ǹ�ҳ�����ӷ��ʹ�����
	private String http_user_agent;// ��¼�ͻ�������������Ϣ
	private boolean valid = true;// �ж������Ƿ�Ϸ�

	public static KPI parser(String line) {
		
		KPI kpi = new KPI();
		String arr[] = line.split(" ");
		if (arr.length > 11) {
			kpi.setRemote_addr(arr[0]);
			kpi.setRemote_user(arr[1]);
			kpi.setTime_local(arr[3].substring(1));
			kpi.setRequest(arr[6]);
			kpi.setStatus(arr[8]);
			kpi.setBody_bytes_sent(arr[9]);
			kpi.setHttp_referer(arr[10]);
			if (arr.length > 12) {
				kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
			} else {
				kpi.setHttp_user_agent(arr[11]);
			}
			if (kpi.getStatus().equals("") || kpi.getStatus().contains("HTTP/1.1") || Integer.parseInt(kpi.getStatus()) >= 400){
				kpi.setValid(false);
			}
		} else {
			kpi.setValid(false);
		}
		return kpi;
	}

	/**
	 * ��page��pv����
	 */
	public static KPI filterPVs(String line) {
		KPI kpi = parser(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/ctp080113.php?tid=1561348");
		pages.add("/popwin_js.php?fid=11");
		pages.add("/popwin_js.php");
		pages.add("/home.php?mod=misc&ac=sendmail&rand=1325607178");
		pages.add("/thread-591867-1-1.html");
		pages.add("/popwin_js.php?fid=6");
		pages.add("/popwin_js.php?fid=131");
		pages.add("/thread-1042683-1-1.html");
		pages.add("/archiver/tid-427627.html");
		if (!pages.contains(kpi.getRequest())) {
			kpi.setValid(false);
		}
		return kpi;
	}

	/**
	 * ��page�Ķ���ip����
	 */
	public static KPI filterIPs(String line) {
		KPI kpi = parser(line);
		Set<String> pages = new HashSet<String>();
		pages.add("203.208.60.242");
		pages.add("180.77.80.73");
		pages.add("123.127.203.195");
		if (!pages.contains(kpi.getRemote_addr())) {
			kpi.setValid(false);
		}
		return kpi;
	}

	/**
	 * PV�����������
	 */
	public static KPI filterBroswer(String line) {
		return parser(line);
	}

	/**
	 * PV��Сʱ����
	 */
	public static KPI filterTime(String line) {
		return parser(line);
	}

	/**
	 * PV��������������
	 */
	public static KPI filterDomain(String line) {
		return parser(line);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nremote_addr:" + this.remote_addr);
		sb.append("\nremote_user:" + this.remote_user);
		sb.append("\ntime_local:" + this.time_local);
		sb.append("\nrequest:" + this.request);
		sb.append("\nstatus:" + this.status);
		sb.append("\nbody_bytes_sent:" + this.body_bytes_sent);
		sb.append("\nhttp_referer:" + this.http_referer);
		sb.append("\nhttp_user_agent:" + this.http_user_agent);
		return sb.toString();
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remoteAddr) {
		remote_addr = remoteAddr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remoteUser) {
		remote_user = remoteUser;
	}

	public String getTimeLocal() {
		return time_local;
	}

	public Date getTime_local_Date() throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
				Locale.US);
		return format.parse(this.time_local);
	}

	public String getTime_local_Date_hour() throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
		return df.format(this.getTime_local_Date());
	}

	public void setTime_local(String timeLocal) {
		time_local = timeLocal;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String bodyBytesSent) {
		body_bytes_sent = bodyBytesSent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	// \"http://www.angularjs.cn/A00n\"
	public String getHttp_referer_domain() {
		if (http_referer.length() < 8) {
			return http_referer;
		} else {
			String str = this.http_referer.replace("\"", "").replace("http://",
					"").replace("https://", "");
			return str.indexOf("/") > 0 ? str.substring(0, str.indexOf("/"))
					: str;
		}
	}

	public void setHttp_referer(String httpReferer) {
		http_referer = httpReferer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String httpUserAgent) {
		http_user_agent = httpUserAgent;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public static void main(String args[]) {
		String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
		System.out.println(line);
		KPI kpi = new KPI();
		String[] arr = line.split(" ");
		kpi.setRemote_addr(arr[0]);
		kpi.setRemote_user(arr[1]);
		kpi.setTime_local(arr[3].substring(1));
		kpi.setRequest(arr[6]);
		kpi.setStatus(arr[8]);
		kpi.setBody_bytes_sent(arr[9]);
		kpi.setHttp_referer(arr[10]);
		kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
		System.out.println(kpi);
		System.out.println("----------------------------��֦��---------------------------------");
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss",
					Locale.US);
			System.out.println(df.format(kpi.getTime_local_Date()));
			System.out.println(kpi.getTime_local_Date_hour());
			System.out.println(kpi.getHttp_referer_domain());
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}