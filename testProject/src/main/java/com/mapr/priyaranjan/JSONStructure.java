package com.mapr.priyaranjan;

import java.util.List;

public class JSONStructure {
	String city;
	String state;
	Double pop;
	String id;
	List<Object> location;
	
	public JSONStructure(String city, String state, Double pop, String id, List<Object> loc) {
		super();
		this.city = city;
		this.state = state;
		this.pop = pop;
		this.id = id;
		this.location = loc;
	}

	public String getCity() {
		return city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Double getPop() {
		return pop;
	}

	public void setPop(Double pop) {
		this.pop = pop;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Object> getLocation() {
		return location;
	}

	public void setLocation(List<Object> location) {
		this.location = location;
	}
	
	
	
	
	
	

}
