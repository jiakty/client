package com.bnsf.kafkatest.chatclient.beans;

public class Message {
	private User user;
	private String message;
	private long time;
	public Message() {
		super();
		// TODO Auto-generated constructor stub
	}
	public User getUser() {
		return user;
	}
	public void setUser(User user) {
		this.user = user;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	@Override
	public String toString() {
		return "Message [user=" + user + ", message=" + message + ", time=" + time + "]";
	}
	
}
