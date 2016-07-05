package it.himyd.stock;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

public class StockCluster implements Serializable {
	private static final long serialVersionUID = 1L;

	private Integer cluster;
	private Date clustertime;
	private String symbol;

	public StockCluster() {
		this.clustertime = new Date();
	}

	public Integer getCluster() {
		return cluster;
	}

	public void setCluster(Integer cluster) {
		this.cluster = cluster;
	}

	public Date getClustertime() {
		return clustertime;
	}

	public void setClustertime(Date clustertime) {
		this.clustertime = clustertime;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	@Override
	public String toString() {
		return "StockCluster [cluster=" + cluster + ", clustertime=" + clustertime + ", symbol=" + symbol + "]";
	}

}
