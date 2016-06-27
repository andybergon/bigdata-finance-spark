package it.himyd.stock;

import java.io.Serializable;
import java.util.Date;

public class StockVariation implements Serializable {

	private static final long serialVersionUID = 1L;

	private String name;
	private Date time;
	private Double priceVariation;
	private Double volumeVariation;

	public StockVariation(String name, Date time, Double priceVariation, Double volumeVariation) {
		super();
		this.name = name;
		this.time = time;
		this.priceVariation = priceVariation;
		this.volumeVariation = volumeVariation;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public Double getPriceVariation() {
		return priceVariation;
	}

	public void setPriceVariation(Double priceVariation) {
		this.priceVariation = priceVariation;
	}

	public Double getVolumeVariation() {
		return volumeVariation;
	}

	public void setVolumeVariation(Double volumeVariation) {
		this.volumeVariation = volumeVariation;
	}

	@Override
	public String toString() {
		return "StockVariation [name=" + name + ", time=" + time + ", priceVariation=" + priceVariation
				+ ", volumeVariation=" + volumeVariation + "]";
	}

}
