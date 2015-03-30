package com.wellsfargo.isg.ofx.event;

import java.util.ArrayList;
import java.util.List;

import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.customer.ServiceStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.FMSServiceAudit;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.logging.Loggers;
/**
 * doing an audit on all suspended customers.  It tries to assign a valid
 * fee account.  Also if a customer is suspended beyond the grace period, the 
 * customer will be suspended.
 *
 * @author Darrell Jefferson
 * @version 1.0
 * @created 12-Apr-2010 3:06:05 PM
 */
public class SuspendedUserAudit extends AbstractAudit {
	
	public SuspendedUserAudit(IFmsRepository res,int fetchSize) {
		super(res,fetchSize);
	}
	
	public String processEvent(String serverId, String eventType){
		Loggers.systemLogger.info("SuspendedUserAudit.process() serverId=" + serverId + " eventType=" + eventType);
		
		try {
			int ret = super.process(eventType,ServiceStatus.SUSPENDED, serverId);
			if(ret == OfxStatus.FAILURE.getCode())
				return String.valueOf(OfxStatus.FAILURE.getCode());
		} catch (Exception e) {
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker("error",e);			
			return String.valueOf(OfxStatus.FAILURE.getCode());
		}
		return String.valueOf(OfxStatus.SUCCESS.getCode());
	}
	protected List<?> findAuditData(int serverId) throws AuditEventException{
		return fmsRepository.findAllFmsCustomerByStatus(ServiceStatus.SUSPENDED, null, null, fetchSize,serverId);
	}
	protected  void storeAuditData(List<?> audits) throws AuditEventException{
		List<FMSServiceAudit> storeAudits = new ArrayList<FMSServiceAudit>();
		for(int i=0;i<audits.size();i++) {
			FMSServiceAudit audit = (FMSServiceAudit)audits.get(i);
			audit.setSuspendServerId(OfxStatus.FAILURE.getCode());
			audit.setReasonCode(ofxHandlerId.getReasonCode());
			audit.setReasonDesc(ofxHandlerId.getReasonDesc());
			/**
			* if the newStatus on Database is SP (suspended) as it's always for Fee Audit,
			* don't update the modifyDate column because the modifyDate is used by the Fee Audit (FMSFeeAccountChange.java) to 
			* decide if the customer is suspended beyond the grace period (> 30 days) or not.  If beyond grace period, the system 
			* has to disable the FMX service.  
			* 
			* It's changable if  the newStatus on Database is NOT SP (suspended) and the current 
			* newStatus on the current session is also NOT SP,
			* 
			*  Please see AbstractOfxHandler.determineModifyDate() for more detail
			*/
			//audit.setModifyDate(Calendar.getInstance());
			
			storeAudits.add(audit);
		}
		fmsRepository.storeFmsServiceStatus(storeAudits);
	}
	protected List<AccountChangeEvent> populateAccountChangeEvent(List<?> audits,String eventType) {
		return super.assembleFmsServiceAudit(audits,
				eventType,ofxGlobal.getCompanyId(),ServiceStatus.SUSPENDED,ServiceStatus.ACTIVE);
	}
}
