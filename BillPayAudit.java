package com.wellsfargo.isg.ofx.event;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.wellsfargo.isg.domain.model.account.Account;
import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.common.ProductCode;
import com.wellsfargo.isg.domain.model.customer.ServiceStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.FMSBillPayAudit;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.logging.Loggers;
/**
 * doing a billpay audit on customers whose status is terminated on Billpay checkfree.
 * 
 * @author Darrell Jefferson
 * @version 1.0
 * @created 12-Apr-2010 3:06:05 PM
 */
public class BillPayAudit extends AbstractAudit{	
	String modifierId;	
	public BillPayAudit(IFmsRepository res,int fetchSize) {
		super(res,fetchSize);				
	}
	public String getModifierId() {
		return modifierId;
	}
	public void setModifierId(String modifierId) {
		this.modifierId = modifierId;
	}
	
	public String processEvent(String serverId, String eventType){
		Loggers.systemLogger.info("BillPayAudit.process() serverId=" + serverId + " eventType=" + eventType);
		
		try {
			int ret = super.process(eventType,ServiceStatus.INACTIVE, serverId);
			if(ret == OfxStatus.FAILURE.getCode())
				return String.valueOf(OfxStatus.FAILURE.getCode());
		} catch (Exception e) {
			com.wellsfargo.isg.util.Util.getOvoMsgBean().logOvoErrorMsgBanker("error",e);
			
			return String.valueOf(OfxStatus.FAILURE.getCode());
		}
		
		return String.valueOf(OfxStatus.SUCCESS.getCode());
	}
	@Override
	protected List<?> findAuditData(int serverId) throws AuditEventException {
		return fmsRepository.findAllFMSBillpayAuditByStatus(ServiceStatus.ACTIVE_SHORT, null, null, fetchSize, serverId,OfxStatus.FAILURE.getCode());
	}
	@Override
	protected List<AccountChangeEvent> populateAccountChangeEvent(
			List<?> audits, String eventType) {
		List<AccountChangeEvent> acctChangeEvent = new ArrayList<AccountChangeEvent>();
		
		for(int i=0;i<audits.size();i++) {
			AccountChangeEvent anEvent = new AccountChangeEvent();			
			FMSBillPayAudit anAudit = (FMSBillPayAudit) audits.get(i);
			
			anEvent.setAuditId(anAudit.getBp_id());
			//eventType is an action 
			anEvent.setAction(eventType);
		
			//new status
			Account newAcctStatus = new Account();
			anEvent.setNewAcctStatus(newAcctStatus);
			anEvent.getNewAcctStatus().getAccountInfo().setServiceStatus(ServiceStatus.findEnumObjByValue(anAudit.getBpStatusCol()));
			anEvent.getNewAcctStatus().getHoganKey().setNumber(anAudit.getXaId());
			anEvent.getNewAcctStatus().getHoganKey().setCompanyId(ofxGlobal.getCompanyId());
			anEvent.getNewAcctStatus().getHoganKey().setProductCode(ProductCode.FMX);
		
			acctChangeEvent.add(anEvent);
		}
		return acctChangeEvent;
	}
	@Override
	protected void storeAuditData(List<?> audits) throws AuditEventException {
		List<FMSBillPayAudit> storeAudits = new ArrayList<FMSBillPayAudit>();
		for(int i=0;i<audits.size();i++) {
			FMSBillPayAudit audit = (FMSBillPayAudit)audits.get(i);
			audit.setServerId(OfxStatus.FAILURE.getCode());
			audit.setErrorId(OfxStatus.SUCCESS.getCode());
			audit.setErrorDesc(OfxStatus.SUCCESS.getStatus());
			audit.setModifyDate(Calendar.getInstance());
			audit.setModifierId(modifierId);
			storeAudits.add(audit);
		}
		fmsRepository.storeFmsBillpayStatus(storeAudits);
		
	}
}
