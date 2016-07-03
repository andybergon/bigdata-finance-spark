package it.himyd.stock;

import java.io.Serializable;
import java.util.Date;

import it.himyd.stock.finance.yahoo.Stock;

public class StockSample implements Serializable {
	private static final long serialVersionUID = 6750153003452580328L;
	
	private String symbol;
	private Date trade_timestamp;
	private Double price;
	
	public StockSample() {
	}
	
	public StockSample(Stock stock) {
		this.symbol = stock.getSymbol();
		this.price = stock.getQuote().getPrice().doubleValue();
		this.trade_timestamp = new Date();
	}

	public StockSample(String symbol, Date trade_timestamp, Double price) {
		super();
		this.symbol = symbol;
		this.trade_timestamp = trade_timestamp;
		this.price = price;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Date getTrade_timestamp() {
		return trade_timestamp;
	}

	public void setTrade_timestamp(Date trade_timestamp) {
		this.trade_timestamp = trade_timestamp;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((price == null) ? 0 : price.hashCode());
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
		result = prime * result + ((trade_timestamp == null) ? 0 : trade_timestamp.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StockSample other = (StockSample) obj;
		if (price == null) {
			if (other.price != null)
				return false;
		} else if (!price.equals(other.price))
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		if (trade_timestamp == null) {
			if (other.trade_timestamp != null)
				return false;
		} else if (!trade_timestamp.equals(other.trade_timestamp))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "StockSample [symbol=" + symbol + ", trade_timestamp=" + trade_timestamp + ", price=" + price + "]";
	}

}
