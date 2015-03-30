package com.wellsfargo.isg.ofx.event;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.wellsfargo.isg.logging.Loggers;
/**
 * @author Darrell Jefferson
 * @version 1.0
 * @created 12-Apr-2010 3:06:05 PM
 */
public class EventServerID {
	public static final String SERVER_ID="eventServerId";
	private String serverId;	
	public EventServerID(String mbeanObjName,String mbeanJndiLoopup,
					String serverRunTime,String serverName,
					String listenAddress, String listenPort) throws MalformedObjectNameException, NullPointerException, NamingException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException {
		try {
		ObjectName service = new ObjectName(mbeanObjName);
		InitialContext ctx = new InitialContext();
		MBeanServer server = (MBeanServer)ctx.lookup(mbeanJndiLoopup);
		ObjectName rt =  (ObjectName)server.getAttribute(service,serverRunTime);
		String hostName=(String)server.getAttribute(rt,serverName);
		String hostAddr=(String)server.getAttribute(rt,listenAddress);
		String port=((Integer)server.getAttribute(rt,listenPort)).toString();
		
		
		//for the ip=192.168.1.66, just take 66 
		String s =hostAddr.substring(hostAddr.lastIndexOf(".")+1);
		serverId = s + "" + port;
		Loggers.systemLogger.info("EventServerID() serverId=" + serverId + " hostname=" + hostName + " hostAddr="+hostAddr + " port=" + port);
		} catch(Exception e) {
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker("error" ,e);
		}
	}
	public String getServerId() {
		return serverId;
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
}
