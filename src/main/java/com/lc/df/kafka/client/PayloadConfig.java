package com.lc.df.kafka.client;

public class PayloadConfig {
	String type;
	String payload;
	public PayloadConfig(String type,String payload){
		this.type = type;
		this.payload = payload;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getPayload() {
		return payload;
	}
	public void setPayload(String payload) {
		this.payload = payload;
	}
	
}
