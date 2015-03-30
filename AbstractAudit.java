package com.wellsfargo.isg.ofx.event;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;

import com.wellsfargo.isg.domain.model.account.Account;
import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.common.ProductCode;
import com.wellsfargo.isg.domain.model.customer.ServiceStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.FMSServiceAudit;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.domain.model.ofx.OfxGlobal;
import com.wellsfargo.isg.domain.model.ofx.OfxHandlerId;
import com.wellsfargo.isg.domain.model.ofx.OvoMsg;
import com.wellsfargo.isg.domain.model.ofx.stats.AuditMonitor;
import com.wellsfargo.isg.logging.Loggers;
import com.wellsfargo.isg.util.XMLGregorianCalendarGenerator;
/**
 * This is the base class that layout the framework on how to do an audit.
 * it gets data from database based on the fetchsize.  The fetchsize basically
 * controls how many threads will be launched. The bigger the fetchsize, the
 * less number of threads.
 * 
 * @author Darrell Jefferson
 * @version 1.0
 * @created 12-Apr-2012 3:06:05 PM
 */
public abstract class AbstractAudit {
	protected IFmsRepository fmsRepository = null;
	protected int fetchSize;
	OfxHandlerId ofxHandlerId;

	@Autowired
	OfxGlobal ofxGlobal;

	@Autowired
	OvoMsg ovoMsg;
	public AbstractAudit(IFmsRepository res,int fetchSize) {
		fmsRepository = res;
		this.fetchSize=fetchSize;  
	}
	
	public OfxHandlerId getOfxHandlerId() {
		return ofxHandlerId;
	}

	public void setOfxHandlerId(OfxHandlerId ofxHandlerId) {
		this.ofxHandlerId = ofxHandlerId;
	}

	/**
	 * how and where to get data for audit
	 *  
	 * @param serverId
	 * @return
	 */
	protected abstract List<?> findAuditData(int serverId) throws AuditEventException;

	/**
	 * once the data is delegrated to a handler for processing, mark it as processed.
	 * 
	 * @param audits
	 * @throws AuditEventException
	 */
	protected abstract void storeAuditData(List<?> audits) throws AuditEventException;

	/**
	 * populate each event's data to AccountChangeEvent
	 * 
	 * @param audits
	 * @return
	 */
	protected abstract List<AccountChangeEvent> populateAccountChangeEvent(List<?> audits,String eventType);

	protected List<AccountChangeEvent> assembleFmsServiceAudit(List<?> audits,
			String eventType,String companyId ,ServiceStatus curStatus,ServiceStatus prevStatus) {
		List<AccountChangeEvent> acctChangeEvent = new ArrayList<AccountChangeEvent>();

		for(int i=0;i<audits.size();i++) {
			AccountChangeEvent anEvent = new AccountChangeEvent();   
			FMSServiceAudit anAudit = (FMSServiceAudit) audits.get(i);
			anEvent.setEventDate(XMLGregorianCalendarGenerator.generateXMLGregorianCalendar( anAudit.getModifyDate().getTime()));

			//eventType is an action 
			anEvent.setAction(eventType);

			Account acctStatus=new Account();
			anEvent.setAcctStatus(acctStatus);


			//new status
			Account newAcctStatus = new Account();
			anEvent.setNewAcctStatus(newAcctStatus);
			anEvent.getNewAcctStatus().getAccountInfo().setServiceStatus(curStatus);
			anEvent.getNewAcctStatus().getHoganKey().setNumber(anAudit.getFmXaCCOUNT());
			anEvent.getNewAcctStatus().getHoganKey().setCompanyId(companyId);
			anEvent.getNewAcctStatus().getHoganKey().setProductCode(ProductCode.FMX);

			Account prevAcctStatus=new Account();
			anEvent.setPrevAcctStatus(prevAcctStatus);
			anEvent.getPrevAcctStatus().getAccountInfo().setServiceStatus(prevStatus);
			//this is used to update the status for auditing suspended and active users, so that
			//a new record won't be created
			anEvent.setFmsServiceAudit(anAudit);
			acctChangeEvent.add(anEvent);
		}
		return acctChangeEvent;
	}



	/**
	 * get data according to the fetchSize from db, then launch a thread for each block of data until
	 * all the data are pulled from db
	 * @param status
	 * @param serverId
	 * @return
	 */
	protected int process(String eventType,ServiceStatus status,String serverId) {  
		Loggers.systemLogger.info("AbstractAudit.process eventType=" + eventType + " serverId=" + serverId);  		
		startMasterAudit( eventType,  serverId);
		return OfxStatus.SUCCESS.getCode();
	}
	
	private void preProcessAuditEvent(String eventType, String serverId) {
		ExecutorService threadPool = Executors.newFixedThreadPool(ofxGlobal.getAuditThreadSize());	
		CompletionService<EventThreadResult> completionService = new ExecutorCompletionService<EventThreadResult>(threadPool);
		EventJobs eJobs = new EventJobs();
		AuditSizeHolder auditSizeHolder = null;
		try {
			Loggers.systemLogger.info("AbstractAudit.preProcessAuditEvent() starts "+ eventType + " serverId=" + serverId + " maxThreadSize="+ofxGlobal.getAuditThreadSize());
			auditSizeHolder=processAuditEvent(completionService,eJobs,eventType, serverId);
			Loggers.systemLogger.info("AbstractAudit.preProcessAuditEvent() ends "+ eventType + " serverId=" + serverId);
		} finally {
			if(threadPool !=null ) {
				threadPool.shutdown();
				Loggers.systemLogger.info("AbstractAudit.preProcessAuditEvent() shutdown() " + " eventType=" + eventType + " serverId=" + serverId );
				//jut to be safe. removing Futures in the Completion queue to prevent memory leak.
				cleanCompletionQueue(completionService,threadPool,eJobs,eventType,serverId);				
				completionService=null;
				threadPool=null;
				logMasteraudit(eventType,auditSizeHolder);
				Loggers.systemLogger.info("AbstractAudit.preProcessAuditEvent() threadpool cleanup done " + " eventType=" + eventType + " serverId=" + serverId );
			}
		
		}
	}
	/**
	 * jut to be safe. removing Futures in the Completion queue to prevent memory leak.
	 * it simply just waits for all submitted jobs to be done, and removes them from the 
	 * Completion queue
	 *  
	 * @param completionService
	 * @param threadPool
	 * @param eJobs
	 */

	private void cleanCompletionQueue(
			CompletionService<EventThreadResult> completionService,ExecutorService threadPool,EventJobs eJobs ,String eventType, String serverId) {
		Loggers.systemLogger.info("AbstractAudit.cleanCompletionQueue() starting...."+ " eventType=" + eventType + " serverId=" + serverId
					+ " curJobSize=" + eJobs.getCount());
		List<EventThreadResult> futures =  new ArrayList<EventThreadResult>() ;
		for(int i=0;i<eJobs.getCount();i++)
		{
			EventThreadResult atask = null;
			try {
				atask=  completionService.take().get();
			} catch (InterruptedException e) {
				atask = null;
			} catch (ExecutionException e) {
				atask=null;
			} 
			if(atask!=null)
				futures.add(atask);

		}		
		if(Loggers.systemLogger.isInfoEnabled()) {
			for(EventThreadResult fe : futures) {			
				Loggers.systemLogger.info("AbstractAudit.cleanCompletionQueue() " + fe.getThreadId()+ " eventType=" + eventType + " serverId=" + serverId);			
			}
		}
		Loggers.systemLogger.info("AbstractAudit.cleanCompletionQueue() ended...."+ " eventType=" + eventType + " serverId=" + serverId);
	}
	class AuditSizeHolder {
		int total =0;
		int numThreads=0;
		int poolSizeSofar = 0;
		String threadId = null;
		public int getTotal() {
			return total;
		}
		public void setTotal(int total) {
			this.total = total;
		}
		public int getNumThreads() {
			return numThreads;
		}
		public void setNumThreads(int numThreads) {
			this.numThreads = numThreads;
		}
		public int getPoolSizeSofar() {
			return poolSizeSofar;
		}
		public void setPoolSizeSofar(int poolSizeSofar) {
			this.poolSizeSofar = poolSizeSofar;
		}
		
		public String getThreadId() {
			return threadId;
		}
		public void setThreadId(String threadId) {
			this.threadId = threadId;
		}
		public void incrementTotal (int t) { total = total +t; }
		public void incrementnumThreads() { numThreads = numThreads+1;}
		public void incrementPoolSizeSofar () {poolSizeSofar=poolSizeSofar+1;}
		
	}
	
	private AuditSizeHolder processAuditEvent(CompletionService<EventThreadResult> completionService,EventJobs eJobs,String eventType, String serverId) {
		Loggers.systemLogger.info("AbstractAudit.processAuditEvent() starts "+ eventType + " serverId=" + serverId);											
		AuditSizeHolder auditSize = new AuditSizeHolder();		
		int idx = 0;
		String threadId ="";
		String threadIdPost = createThrIdPost();
		
		while(true) {
			try {
				List<?> audits = this.findAuditData(Integer.parseInt(serverId));    
				if(audits == null)
					break;
				else if(audits.size()==0)
					break;
				else {		
					idx=idx+1;
					threadId = eventType+idx+threadIdPost;
					
					//max out the pool first
					if(auditSize.getPoolSizeSofar() <ofxGlobal.getAuditThreadSize()) {
						submitJob(audits,completionService,eJobs,threadId,eventType,serverId,auditSize);						
						auditSize.incrementPoolSizeSofar();
						if(Loggers.systemLogger.isInfoEnabled())
							Loggers.systemLogger.info("AbstractAudit.processAuditEvent() submitting job since pool is NOT maxed threadId="
									+threadId + " currentEventJobSize=" + eJobs.getCount());
					}
					else {
						while(true) {
							EventPoolResult result= checkEventThread(completionService,eJobs,threadId);
							if(result.isCreateMore()) {			
								submitJob(audits,completionService,eJobs,threadId,eventType,serverId,auditSize);													
								if(Loggers.systemLogger.isInfoEnabled())
									Loggers.systemLogger.info("AbstractAudit.processAuditEvent() 1 more job is submitted in whileLoop eventType=" 
											+ eventType + " serverId=" + serverId + " threadId=" + threadId);											
								//get out of the inner while loop once a thread is created
								break;
							}							
						} // inner while
					} //end of else
				}//end of else
			} catch(AuditEventException e) { 
				ovoMsg.logOvoErrorMsgBanker(" threadId=" + threadId + " serverId=" + serverId+" exception",e);        
			}
		}//end of while
				
		if(Loggers.systemLogger.isInfoEnabled()) {
			Loggers.systemLogger.info("AbstractAudit.process() total records=" + auditSize.getTotal() + " eventType=" + eventType + " serverId=" + serverId );
			Loggers.systemLogger.info("AbstractAudit.process() total threads=" + auditSize.getNumThreads() + " eventType=" + eventType + " serverId=" + serverId );
			Loggers.systemLogger.info("AbstractAudit.processAuditEvent() ends "+ eventType + " serverId=" + serverId);
		}
		auditSize.setThreadId(threadId);
		return auditSize;
	}
	private void logMasteraudit(String eventType, AuditSizeHolder auditSizeHolder) {
		Loggers.systemLogger.info("AbstractAudit.logMasteraudit() starts eventType="+eventType );
		AuditMonitor am = new AuditMonitor();
		am.setNumOfCustomerProcessed(auditSizeHolder.getTotal());
		am.setEventType(eventType);
		am.setThreadId(auditSizeHolder.getThreadId());
		am.setThreadCounts(auditSizeHolder.getNumThreads());
		am.setThreadType(AuditMonitor.THREAD_TYPE.master.name());
		am.setAuditMonitorCode(OfxStatus.MASTER_AUDIT_MONITOR);
		try {
			this.fmsRepository.storeAuditMonitor(am);
		} catch (AuditEventException e) {
			
		}
		Loggers.systemLogger.info("AbstractAudit.logMasteraudit() ends eventType="+eventType );
	}
	private void submitJob(List<?> audits,CompletionService<EventThreadResult> completionService,EventJobs eJobs,
			String threadId,String eventType,String serverId,AuditSizeHolder auditSize) throws AuditEventException {
		if(Loggers.systemLogger.isInfoEnabled()) {
			Loggers.systemLogger.info("AbstractAudit.submitJob() eventType=" + eventType + " serverId=" + serverId + " creates 1 more thread with threadId=" + threadId);
			Loggers.systemLogger.info("AbstractAudit.submitJob() eventType=" + eventType + " serverId=" + serverId + " total number of threads=" +auditSize.getNumThreads());
			Loggers.systemLogger.info("AbstractAudit.submitJob() eventType=" + eventType + " serverId=" + serverId + " with data block size=" + audits.size());
		}
		List<AccountChangeEvent> acctChangeEvent = this.populateAccountChangeEvent(audits,eventType);
		EventThread eventThr = new EventThread(eventType,Integer.parseInt(serverId),acctChangeEvent,threadId,fmsRepository);		
		completionService.submit( eventThr ) ;
		
		//mark the block as processed
		this.storeAuditData(audits);
		
		auditSize.incrementTotal(audits.size());
		auditSize.incrementnumThreads();
		eJobs.increment();
		
		Loggers.systemLogger.info("AbstractAudit.submitJob()  submitting one more job totalJobInProcess=" + eJobs.getCount()
				+ " with threadId=" + threadId + " eventType=" + eventType + " serverId=" + serverId );
	}
	private String createThrIdPost() {
		Calendar cal=Calendar.getInstance();
		String threadIdPost="-" +  (cal.get(Calendar.MONTH)+1) +"/"+ cal.get(Calendar.DATE) 
				+":H" + cal.get(Calendar.HOUR_OF_DAY)
				+":M"+ cal.get(Calendar.MINUTE)
				+":S"+cal.get(Calendar.SECOND)
				;
		return threadIdPost;
	}
	
	private EventPoolResult checkEventThread(CompletionService<EventThreadResult> completionService,EventJobs eJobs,String threadId )  {
		EventPoolResult result = new EventPoolResult();
		try {
		
			for(int i=0;i<eJobs.getCount();i++) {
				if(Loggers.systemLogger.isInfoEnabled()) 				
						Loggers.systemLogger.info("AbstractAudit.checkEventThread() waiting for a job to be done currentSize=" + eJobs.getCount()
								+" threadId=" + threadId);
				Future<EventThreadResult> future = completionService.take();				
				if(future!=null) {						
					result.setCreateMore(true);
					result.addFuture(future);
					if(eJobs.getCount()>0) {
						eJobs.decrement();
						
						if(Loggers.systemLogger.isInfoEnabled()) {
							try {
								Loggers.systemLogger.info("AbstractAudit.checkEventThread() removes a job currentSize=" + eJobs.getCount()+" job done with threadId=" + future.get().getThreadId());
							} catch (ExecutionException ignore) {
							}
						}
					}
					if(Loggers.systemLogger.isInfoEnabled())
						Loggers.systemLogger.info("AbstractAudit.checkEventThread()  completionService job is done " +" threadId=" + threadId);
					
					break;
				}
			}
		} catch (InterruptedException e) {
			result.setCreateMore(false);
		} 
		if(Loggers.systemLogger.isInfoEnabled())
			Loggers.systemLogger.info("AbstractAudit.checkEventThread()  createMore job = " + result.isCreateMore()+" threadId=" + threadId);
		
		return result;

	}
	class EventJobs {	
		int count = 0;
		public int getCount() {return count;}
		public void setCount(int count) {this.count = count;}
		public void increment() { count=count+1;}
		public void decrement() {count=count-1;}
	}
	class EventPoolResult {
		boolean createMore = false;
		List<Future<EventThreadResult>> futures =   
			new ArrayList<Future<EventThreadResult>>() ;  
		public boolean isCreateMore() {
			return createMore;
		}
		public void setCreateMore(boolean createMore) {
			this.createMore = createMore;
		}
		public List<Future<EventThreadResult>> getFutures() {
			return futures;
		}
		public void setFutures(List<Future<EventThreadResult>> futures) {
			this.futures = futures;
		}
		public void addFuture(Future<EventThreadResult> future) {
			futures.add(future);
		}

	}
	private void startMasterAudit(String eventType, String serverId) {
		ExecutorService pool = null;
		String masterThreadId ="Master-" +eventType+ createThrIdPost();
		try {			
			
			MasterAudit ma = new MasterAudit(eventType, serverId,masterThreadId);
			pool = Executors.newSingleThreadExecutor();
			pool.submit(ma);
			Loggers.systemLogger.info("AbstractAudit.startMasterAudit() a job is submitted..."+ " masterAuditThreadId="+masterThreadId
					+ " serverId=" + serverId);
		} catch(Exception ignore) {
			Loggers.systemLogger.info("AbstractAudit.startMasterAudit() "+ " masterAuditThreadId="+masterThreadId
					+ " serverId=" + serverId,ignore);
		} finally {
			if(pool !=null) {
				pool.shutdown();
				pool=null;
				Loggers.systemLogger.info("AbstractAudit.startMasterAudit() shutdown() pool "+ " masterAuditThreadId="+masterThreadId
						+ " serverId=" + serverId);
			}

		}
	}
	class MasterAudit implements Runnable {
		String eventType; String serverId;
		String masterThreadId ="";
		public MasterAudit(String _eventType, String _serverId,String _masterThreadId) {
			eventType=_eventType;
			serverId = _serverId;
			masterThreadId =_masterThreadId;
		}
		@Override
		public void run() {
			try {
				Loggers.systemLogger.info("AbstractAudit.MasterAudit() starts audit event " + " masterAuditThreadId="+masterThreadId
						+ " serverId=" + serverId);
				preProcessAuditEvent(eventType, serverId);
				Loggers.systemLogger.info("AbstractAudit.MasterAudit() ends audit event " + " masterAuditThreadId="+masterThreadId
						+ " serverId=" + serverId);
			} catch(Exception e) {
				ovoMsg.logOvoErrorMsgBanker( 
						" MasterAuditThread.EventTask.run() "+ " masterAuditThreadId="+masterThreadId
						+ " serverId=" + serverId, 
						e);
			} 
		}

	} //end of MasterAudit inner class
}
