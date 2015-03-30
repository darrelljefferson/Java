package com.wellsfargo.isg.ofx.event;

public class EventThreadResult {
	int size = 0;
	String threadId ;
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public String getThreadId() {
		return threadId;
	}

	public void setThreadId(String threadId) {
		this.threadId = threadId;
	}
}
