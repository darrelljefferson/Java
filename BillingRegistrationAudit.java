package com.wellsfargo.isg.ofx.event;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.customer.ServiceStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.FMSServiceAudit;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.logging.Loggers;
/**
 * doing billing registration audit to see if the characteristic of a customer
 * has changed, i.e. a customer uses billpay now, but not before; a customer uses
 * quickbook now, but not before, etc...
 *  
 * @author Darrell Jefferson
 * @version 1.0
 * @created 12-Apr-2012 3:06:05 PM
 */
public class BillingRegistrationAudit extends AbstractAudit{	
	public BillingRegistrationAudit(IFmsRepository res,int fetchSize) {
		super(res,fetchSize);
	}
	
	public String process(String serverId, String eventType) {
		Loggers.systemLogger.info("BillingRegistrationAudit.process() serverId=" + serverId + " eventType=" + eventType);		
		try {
			int ret = super.process(eventType,ServiceStatus.ACTIVE, serverId);
			if(ret == OfxStatus.FAILURE.getCode())
				return String.valueOf(OfxStatus.FAILURE.getCode());
		} catch(Exception e) {
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker("BillingRegistrationAudit.process()",e);			
			return String.valueOf(OfxStatus.FAILURE.getCode());
		}
		return String.valueOf(OfxStatus.SUCCESS.getCode());
	}
	
	protected List<?> findAuditData(int serverId) throws AuditEventException {
		return fmsRepository.findAllFmsCustomerByStatus(ServiceStatus.ACTIVE, null, null, fetchSize,serverId);
	}
	protected  void storeAuditData(List<?> audits) throws AuditEventException {
		List<FMSServiceAudit> storeAudits = new ArrayList<FMSServiceAudit>();
		for(int i=0;i<audits.size();i++) {
			FMSServiceAudit audit = (FMSServiceAudit)audits.get(i);
			audit.setBillingServerId(OfxStatus.FAILURE.getCode());
			audit.setModifyDate(Calendar.getInstance());
			audit.setReasonCode(ofxHandlerId.getReasonCode());
			audit.setReasonDesc(ofxHandlerId.getReasonDesc());
			storeAudits.add(audit);
		}
		fmsRepository.storeFmsServiceStatus(storeAudits);
	}
	protected List<AccountChangeEvent> populateAccountChangeEvent(List<?> audits,String eventType) {
		
		return super.assembleFmsServiceAudit(audits,
				eventType,ofxGlobal.getCompanyId(),ServiceStatus.ACTIVE,ServiceStatus.INACTIVE);
		
	}
	
}
