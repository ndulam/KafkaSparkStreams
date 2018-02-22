package com.naresh.org;

public class Geo 
{
	private Double lat;
	private Double longi;
	public Double getLat() {
		return lat;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	public Double getLongi() {
		return longi;
	}
	public void setLongi(Double longi) {
		this.longi = longi;
	}
	public Geo(Double lat, Double longi) {
		super();
		this.lat = lat;
		this.longi = longi;
	}
	@Override
	public String toString() {
		return "Geo [lat=" + lat + ", longi=" + longi + "]";
	}

}
