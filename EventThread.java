package com.wellsfargo.isg.ofx.event;


import java.util.List;
import java.util.concurrent.Callable;

import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.domain.model.ofx.stats.AuditMonitor;
import com.wellsfargo.isg.logging.Loggers;
import com.wellsfargo.isg.ofx.OFXProcessor;
import com.wellsfargo.isg.util.XmlCoderUtil;
/**
 * a thread that processes an assigned number of transaction.  Basically, it
 * doesn't do any biz logic which will delegated to a corresponding OFX handler
 * that does the actual use case processing.
 * 
 * Corresponding to each Timer object is a single background thread that is 
 * used to execute all of the timer's tasks
 * 
 * see jdk javadoc on Timer class for more detail
 * 
 * @author Darrell Jefferson
 * @version 1.0
 * @created 22-Apr-2010 3:37:48 PM
 * 	
 */
public class EventThread implements Callable<EventThreadResult>{	
	int serverId=0;
	String eventType = null;
	List<AccountChangeEvent> changeEvents;
	String threadId = null;	
	IFmsRepository fmsRepository;
	AuditMonitor am = new AuditMonitor();
	public EventThread(String eventType,int _id,List<AccountChangeEvent> events,String thrId,IFmsRepository fmsRepository) {
		this.eventType = eventType;
		this.serverId=_id;
		this.changeEvents=events;
		threadId = thrId;		
		this.fmsRepository=fmsRepository;
	}

	public String getThreadId() {
		return threadId;
	}
	@Override
	public EventThreadResult call() throws Exception {
		try {
			
			if(changeEvents !=null &&changeEvents.size()!=0) {
				am.setNumOfCustomerProcessed(changeEvents.size());
				am.setEventType(eventType);
				am.setThreadId(threadId);
				am.setThreadType(AuditMonitor.THREAD_TYPE.eventThread.name());
				for(int i=0;i<changeEvents.size();i++) {
					AccountChangeEvent anEvent =null;
					try {
						anEvent =changeEvents.get(i);
						new OFXProcessor().process(anEvent);	
						if(Loggers.systemLogger.isInfoEnabled())
							Loggers.systemLogger.info("EventThread.EventTask.run() eventType=" + eventType + " threadId=" + threadId  + " serverId="+serverId+ " process handler=" + anEvent.getAction());
					} catch(Throwable te) {
						//catch the single event, so that it won't blow up the entire list
						//there's no need to do retry since OFXProcessor would do retry if there's error.
						//I can't imagine any reason it would fail on doing new OFXProcessor().  anyway, just ovo it if there's 
						//then continue onto the next event on the List
						com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker(
								 " EventThread.EventTask.run() eventType=" + eventType + " threadId=" + threadId 
								+ " serverId="+serverId
								+ " processing error xml=" + XmlCoderUtil.objectToXml(anEvent),te);
					}
				} //end of for
			}				

		} catch(Exception e) {
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker(
					 " eventType=" + eventType + " threadId=" + threadId + " serverId="+serverId ,e);
		} finally {

			Loggers.systemLogger.info("EventThread.EventTask.run() terminating timer and thread...eventType=" + eventType + " threadId=" + threadId  + " serverId="+serverId);
			try {
				am.setAuditMonitorCode(OfxStatus.AUDIT_MONITOR);
				fmsRepository.storeAuditMonitor(am);
			} catch (AuditEventException ignore) {

			}
		}
		EventThreadResult result = new EventThreadResult();
		result.setSize(changeEvents.size());
		result.setThreadId(threadId);
		return result;
	}

}
