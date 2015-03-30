package com.wellsfargo.isg.ofx.event;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.wellsfargo.isg.logging.Loggers;
/**
 * 
 * @author Darrell Jefferson
 *
 * clean up and process assigned events for BILLING Audit on server startup.  
 * There’s no need to clean up RETRY, FEE Audit, and Billpay audit since they 
 * are the hourly and daily job.  So the delay is very minimal.  
 * So just clean up billing audit which is monthly
 */
public class EventCleanerOnStartup {
	List _audits ;
	EventServerID _sid;
	public EventCleanerOnStartup(List audits ,com.wellsfargo.isg.ofx.event.EventServerID sid) {
		try {
			//Hibernate exception when starting the audit on constructor, don't know why?

			/*Loggers.systemLogger.info("EventCleanerOnStartup.EventCleanerOnStartup() constructor...");
			EventCleanupThread thr = new EventCleanupThread(audits,sid);

			thr.start();
			 */
			this._audits = audits;
			this._sid=sid;
		} catch(Exception ignore) {
			Loggers.systemLogger.info("EventCleanerOnStartup ",ignore);
		}
	}
	public void start() {
		ExecutorService pool = null;
		try {			
			EventCleanupThread thr = new EventCleanupThread(_audits,_sid);
			pool = Executors.newSingleThreadExecutor();
			pool.submit(thr);
			Loggers.systemLogger.info("EventCleanerOnStartup a job is submitted...");
		} catch(Exception ignore) {
			Loggers.systemLogger.info("EventCleanerOnStartup.start() ",ignore);
		} finally {
			if(pool !=null) {
				pool.shutdown();
				pool=null;
				Loggers.systemLogger.info("EventCleanerOnStartup shutdown() pool");
			}

		}
	}

	class EventCleanupThread implements Runnable
	{		
		List audits;
		EventServerID sid;
		public EventCleanupThread(List _audits ,com.wellsfargo.isg.ofx.event.EventServerID _sid) {
			this.audits =_audits;
			this.sid = _sid;				
		}
		
		@Override
		public void run() {
			try {
				String serverId = sid.getServerId();
				for(int i=0;i<audits.size();i++) {
					EventCleanupBean cleanup = (EventCleanupBean) audits.get(i);
					Loggers.systemLogger.info("EventCleanerOnStartup.EventTask eventType=" + cleanup.getEventType());
					cleanup.anAudit.processEvent(serverId, cleanup.getEventType());
				}

			} catch(Exception ignore) {
				Loggers.systemLogger.info("EventCleanupThread.EventTask.run() " , ignore);
			} 
		}

	}
}
