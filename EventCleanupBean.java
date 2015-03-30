package com.wellsfargo.isg.ofx.event;

import com.wellsfargo.isg.domain.model.common.event.IScheduleEvent;

public class EventCleanupBean {
	String eventType;
	IScheduleEvent anAudit;
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public IScheduleEvent getAnAudit() {
		return anAudit;
	}
	public void setAnAudit(IScheduleEvent anAudit) {
		this.anAudit = anAudit;
	}
	
}
