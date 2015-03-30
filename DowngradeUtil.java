/**
 * 
 */
package com.wellsfargo.isg.ofx.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Calendar;

import com.wellsfargo.isg.domain.model.common.OfxStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.AuditEventException;
import com.wellsfargo.isg.domain.model.ofx.FMSServiceAudit;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;
import com.wellsfargo.isg.logging.Loggers;
import com.wellsfargo.isg.ofx.OFXProcessorException;
import com.wellsfargo.isg.ofx.handler.AbstractOfxHandler;
import com.wellsfargo.isg.ofx.handler.DowngradeHandler;
import com.wellsfargo.isg.util.webapp.SpringApplicationContext;

public class DowngradeUtil extends AbstractUtil {

	Calendar startDate;
	Calendar endDate;
	/**
	 * 
	 */
	public DowngradeUtil(IFmsRepository res,int fetchSize) {
		// TODO Auto-generated constructor stub
		super(res,fetchSize);				
	}
	public String processEvent(String serverId, String eventType, Calendar start, Calendar end){
		Loggers.systemLogger.info("DowngradeUtil.process() serverId=" + serverId + " eventType=" + eventType);
		this.startDate = start;
		this.endDate = end;
		try {
			int ret = super.process(eventType, serverId);
			if(ret == OfxStatus.FAILURE.getCode())
				return String.valueOf(OfxStatus.FAILURE.getCode());
		} catch (Exception e) {
			Loggers.systemLogger.error("DowngradeUtil.process()Exception ",e);
			
			return String.valueOf(OfxStatus.FAILURE.getCode());
		}
		
		return String.valueOf(OfxStatus.SUCCESS.getCode());
	}

	@Override
	protected List<?> findServiceData(int serverId) throws AuditEventException {
		if(fmsRepository == null){
			Loggers.systemLogger.error("DowngradeUtil.findServiceData()autowired fmsRepository is null ");
			//let it throw null point exception
		}
		Loggers.systemLogger.info("Get customer modified between:" + startDate.get(Calendar.MONTH)+"/"+ startDate.get(Calendar.DAY_OF_MONTH)+"/"+ startDate.get(Calendar.YEAR) + " and "  + endDate.get(Calendar.MONTH)+"/"+ endDate.get(Calendar.DAY_OF_MONTH)+"/"+ endDate.get(Calendar.YEAR));
		return fmsRepository.findAllFMSBillingByDate( startDate, endDate, fetchSize, serverId);
		
	}

	@Override
	/**
	 * find a down grade use case handler 
	 * 
	 * @throws OFXProcessorException
	 */
	protected AbstractOfxHandler createOfxHandler() throws OFXProcessorException {
		DowngradeHandler handler = (DowngradeHandler) SpringApplicationContext.getApplicationContext()
				.getBean("DowngradeHandler");
		if (handler == null){
			throw new OFXProcessorException("OFXProcessor.createOfxHandler() handler not found for downgrade action");
		}
		Loggers.systemLogger.info("DowngradeUtil.createOfxHandler() action=downgrade"+ " handler=" + handler.getClass().getName());
		return handler;
	}

	@Override
	protected List<AccountChangeEvent> populateAccountChangeEvent(
			List<?> ServiceData, String eventType) {
		List<AccountChangeEvent> acctChangeEvents = new ArrayList<AccountChangeEvent>();
		FMSServiceAudit aService = null;
		for(int i=0;i<ServiceData.size();i++) {
			AccountChangeEvent anEvent = new AccountChangeEvent();			
			aService = (FMSServiceAudit) ServiceData.get(i);
			//eventType is an action 
			anEvent.setAction(eventType);
			anEvent.setRetry(false);
			anEvent.setFmsServiceAudit(aService);
				
			acctChangeEvents.add(anEvent);
		}
		return acctChangeEvents;
	}
}
