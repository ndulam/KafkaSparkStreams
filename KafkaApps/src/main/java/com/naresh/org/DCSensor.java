package com.naresh.org;

import java.util.Map;

public class DCSensor 
{
	
	private String dcname;
	private Map<String, Sensor>  source;
	
	public DCSensor(String dcname, Map<String, Sensor> source) {
		super();
		this.dcname = dcname;
		this.source = source;
	}

	public String getDcname() {
		return dcname;
	}

	public void setDcname(String dcname) {
		this.dcname = dcname;
	}

	public Map<String, Sensor> getSource() {
		return source;
	}

	public void setSource(Map<String, Sensor> source) {
		this.source = source;
	}
	
}


