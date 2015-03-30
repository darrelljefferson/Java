/**
 * 
 */
package com.wellsfargo.isg.ofx.event;

import java.util.List;

import com.wellsfargo.isg.domain.model.customer.ServiceStatus;
import com.wellsfargo.isg.domain.model.ofx.AccountChangeEvent;
import com.wellsfargo.isg.domain.model.ofx.IFmsRepository;


public class PreBillingAudit extends BillingRegistrationAudit {

	/**
	 * @param res
	 * @param fetchSize
	 */
	public PreBillingAudit(IFmsRepository res, int fetchSize) {
		super(res, fetchSize);
		// TODO Auto-generated constructor stub
	}

	protected List<AccountChangeEvent> populateAccountChangeEvent(List<?> audits,String eventType) {
		String previewEventType = "prebilling";
//		This class is used only for the preview billing registration event.
//		override any other event type.
		return super.assembleFmsServiceAudit(audits,
				previewEventType,ofxGlobal.getCompanyId(),ServiceStatus.ACTIVE,ServiceStatus.INACTIVE);
		
	}
}
