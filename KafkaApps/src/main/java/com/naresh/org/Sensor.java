package com.naresh.org;

public class Sensor 
{
	private int id;
	private String ip;
	private String description;
	private int temp;
	private int c02_level;
	private Geo geo;
	private long eventTime;
	@Override
	public String toString() {
		return "Sensor [id=" + id + ", ip=" + ip + ", description=" + description + ", temp=" + temp + ", c02_level="
				+ c02_level + ", geo=" + geo + ", eventTime=" + eventTime + "]";
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public int getTemp() {
		return temp;
	}
	public void setTemp(int temp) {
		this.temp = temp;
	}
	public int getC02_level() {
		return c02_level;
	}
	public void setC02_level(int c02_level) {
		this.c02_level = c02_level;
	}
	public Geo getGeo() {
		return geo;
	}
	public void setGeo(Geo geo) {
		this.geo = geo;
	}
	public Sensor(int id, String ip, String description, int temp, int c02_level, Geo geo,long eventTime) {
		super();
		this.id = id;
		this.ip = ip;
		this.description = description;
		this.temp = temp;
		this.c02_level = c02_level;
		this.geo = geo;
		this.eventTime = eventTime;
	}
	public long getEventTime() {
		return eventTime;
	}
	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}
	
	
}
