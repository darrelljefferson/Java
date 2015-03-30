package com.wellsfargo.isg.ofx.event;

import java.util.ArrayList;
import java.util.List;

import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.customer.ServiceStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.domain.model.ofx.RequestAudit;
import com.wellsfargo.isg.logging.Loggers;
import com.wellsfargo.isg.util.XmlCoderUtil;
/**
 * a retry audit scans for any transaction that was not successful on performing the use case.  Each
 * retry has three chances.
 * 
 * @author Darrell Jefferson
 * @version 1.0
 * @created 12-Apr-2010 3:06:05 PM
 */
public class RetryAudit extends AbstractAudit {	
	int maxRetryCount;		
	public RetryAudit(IFmsRepository res,int count,int fetchSize) {
		super(res,fetchSize);		
		maxRetryCount=count;
		
	}
	
	public String processEvent(String serverId, String eventType){
		Loggers.systemLogger.info("RetryAudit.process() serverId=" + serverId + " eventType=" + eventType);
		try {
			int ret=super.process(eventType,ServiceStatus.RETRY, serverId);
			if(ret == OfxStatus.FAILURE.getCode())
				return String.valueOf(OfxStatus.FAILURE.getCode());
		} catch(Exception e) {
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker("error",e);			
			return String.valueOf(OfxStatus.FAILURE.getCode());
		}
		return String.valueOf(OfxStatus.SUCCESS.getCode());
	}
	@Override
	protected List<?> findAuditData(int serverId) throws AuditEventException{
		return fmsRepository.findAllRetriableRecord(maxRetryCount, fetchSize, serverId, OfxStatus.FAILURE.getCode());
	}
	@Override
	protected List<AccountChangeEvent> populateAccountChangeEvent(List<?> audits,String eventType) {
		List<AccountChangeEvent> events = new ArrayList<AccountChangeEvent>();		
		
		Class aClass=AccountChangeEvent.class;
		for(int i=0;i<audits.size();i++) { 
			RequestAudit reqAudit = (RequestAudit) audits.get(i);
			AccountChangeEvent anEvent = (AccountChangeEvent)XmlCoderUtil.xmlToObject(reqAudit.getRequestXml(), aClass);
			anEvent.setCreateDate(reqAudit.getCreateDate());
			anEvent.setRequestAudit(reqAudit);
			anEvent.setAuditEventType(eventType);	
			anEvent.setErrorMsg(reqAudit.getErrorDesc());
			anEvent.setRetry(true);			
			events.add(anEvent);
		}
		return events;
	}
	@Override
	protected void storeAuditData(List<?> audits) throws AuditEventException{
		try {
			List<RequestAudit> storeAudits = new ArrayList<RequestAudit>();
			for(int i=0;i<audits.size();i++) {
				RequestAudit audit = (RequestAudit)audits.get(i);
				//only set serverId=-1, so that it won't be processed by the next thread assigned for this
				//server.  However, don't set the error_id yet which will be set by the OFXProcessorHelper 
				//based on the processing status (success or failure)
				audit.setServerId(OfxStatus.FAILURE.getCode());
				storeAudits.add(audit);
			}
			fmsRepository.storeRequestAudit(storeAudits);
		} catch(AuditEventException e) {			
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsg("retryAudit",e);
		}
	}
}
