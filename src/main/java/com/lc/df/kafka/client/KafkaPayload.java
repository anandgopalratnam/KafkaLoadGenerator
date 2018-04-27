package com.lc.df.kafka.client;

public class KafkaPayload {
	String key;
	String value;
	String type;
	public KafkaPayload(String key,String type,String value){
		this.key = key;
		this.type = type;
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("Key :[").append(key).append("] Value [").append(value).append("] Type [")
				.append(type).append("]").toString();
	}
}
