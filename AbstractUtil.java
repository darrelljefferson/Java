/**
 * 
 */
package com.wellsfargo.isg.ofx.event;

import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.customer.Customer;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.logging.Loggers;
import com.wellsfargo.isg.ofx.OFXProcessorException;
import com.wellsfargo.isg.ofx.handler.AbstractOfxHandler;
import com.wellsfargo.isg.ofx.handler.OfxHandlerException;

/**
 * @author a493898
 *
 */
public abstract class AbstractUtil {
	protected IFmsRepository fmsRepository = null;
	protected int fetchSize;

	
	public AbstractUtil(IFmsRepository res,int fetchSize) {
		fmsRepository = res;
		this.fetchSize=fetchSize;  
	}
	
	/**
	 * how and where to get data for utility work
	 *  
	 * @param serverId
	 * @return
	 */
	protected abstract List<?> findServiceData(int serverId) throws AuditEventException;
	/**
	 * populate each event's data to AccountChangeEvent
	 * 
	 * @param audits
	 * @return
	 */
	protected abstract List<AccountChangeEvent> populateAccountChangeEvent(List<?> audits,String eventType);
	
//	protected abstract void storeServiceData(List<?> audits) throws AuditEventException;
	
	protected abstract AbstractOfxHandler createOfxHandler() throws OFXProcessorException;
	/**
	 * get data,  then launch a thread
	 * @param status
	 * @param serverId
	 * @return
	 */
	protected int process(String eventType,String serverId) {  
		Loggers.systemLogger.info("AbstractUtil.process eventType=" + eventType + " serverId=" + serverId);  		
		MasterUtil master = new MasterUtil(eventType, serverId);
		master.start();
		return OfxStatus.SUCCESS.getCode();
	}
	private String createThrIdSuffix() {
		Calendar cal=Calendar.getInstance();
		String threadIdSuffix="-" +  (cal.get(Calendar.MONTH)+1) +"/"+ cal.get(Calendar.DATE) +":H" + cal.get(Calendar.HOUR_OF_DAY);

		return threadIdSuffix;
	}
	private void processUtilEvent(String eventType, String serverId) {
		int total =0;
		int downgraded = 0;
		int iServer = -1;
		try {
			iServer = Integer.parseInt(serverId);
		}catch (NumberFormatException e) {
			//eat the format exception
			Loggers.systemLogger.info(" server Id number format exception",e);   
			iServer = -1;
		} 
		try {
			List<?> serviceData = this.findServiceData(iServer);
			if (serviceData != null && serviceData.size() > 0){
				Loggers.systemLogger.info("total customer to process: " + serviceData.size());
				List<AccountChangeEvent> serviceEvents = this.populateAccountChangeEvent(serviceData,eventType);
				AbstractOfxHandler handler = this.createOfxHandler();
				
				AccountChangeEvent anEvent=null;
				total = serviceEvents.size();
				for(int i=0;i<serviceEvents.size();i++) {
					anEvent =serviceEvents.get(i);
					try {
						//call event handler to process the event
						Customer downgradedCustomer = handler.process(anEvent);
						if (downgradedCustomer == null){
							Loggers.systemLogger.error(" fail to downgrade product for  "+anEvent.getFmsServiceAudit().getFmXaCCOUNT());        
						}else {
							downgraded++;
						}
					} catch (OfxHandlerException e) {
						Loggers.systemLogger.error(" Exception processing change event. ",e);        
					}	
				} //end of for
			}
		} catch (AuditEventException e) {
			Loggers.systemLogger.error(" Exception getting FMS Service Audit data. ",e);        
		} catch (OFXProcessorException e) {
			Loggers.systemLogger.error(" Exception getting OFX handler. ",e);        
		}  
		Loggers.systemLogger.info("AbstractUtil.processUtilEvent() processed "+downgraded+ " of total records=" + total + " eventType=" + eventType + " serverId=" + serverId );
	}
	
	class MasterUtil {
		String eventType; String serverId;
		String masterThreadId ="";
		public MasterUtil(String _eventType, String _serverId) {
			eventType=_eventType;
			serverId = _serverId;
			masterThreadId ="Master-" +eventType+ createThrIdSuffix();
		}
		public void start() {
			try {
				Loggers.systemLogger.info("Begin MasterUtil.start()  eventType=" + eventType + " serverId=" + serverId);
				MasterUtilThread thr = new MasterUtilThread();
				thr.start();
				Loggers.systemLogger.info("MasterUtilThread started...eventType=" + eventType + " serverId=" + serverId);
			} catch(Exception e) {
				Loggers.systemLogger.error("MasterUtil.start()Exception ",e);
			}
		}
		class MasterUtilThread
		{
			Timer timer =null;
			public MasterUtilThread() {
				timer = new Timer(masterThreadId);		
			}
			public void start() {
		    	Loggers.systemLogger.info("Begin MasterUtilThread.start() task scheduled...eventType=" + eventType + " serverId=" + serverId + " MasterThreadId="+masterThreadId);
				TimerTask task = new EventTask();		
		    	timer.schedule(task, 0);
			}
			
			class EventTask extends TimerTask
			{
				@Override
				public void run() {
					try {
				    	Loggers.systemLogger.info("Event task running...eventType=" + eventType + " serverId=" + serverId + " MasterThreadId="+masterThreadId);
						//run the utility event
						processUtilEvent(eventType, serverId);
					} catch(Exception e) {
						Loggers.systemLogger.error("MasterUtilThread.EventTask.run() "+ " MasterThreadId="+masterThreadId , e);
					} finally {
						//terminate this timer and timer's associated thread
						timer.cancel();
						timer=null;
						
						Loggers.systemLogger.info("MasterUtilThread.EventTask.run() terminating timer and thread...eventType=" + eventType + " serverId=" + serverId+ " MasterThreadId="+masterThreadId);
					}
				}
			}
		} //end of MasterUtilThread 
	} //end of MasterUtil inner class
}

