
package it.himyd.finance.yahoo.quotes.stock;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.TimeZone;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import it.himyd.finance.yahoo.Utils;

@JsonIgnoreProperties(ignoreUnknown=true)
public class StockQuote implements Serializable {
    
	private static final long serialVersionUID = 1L;

	private String symbol;
    
    private TimeZone timeZone;
    
    private BigDecimal ask;
    private int askSize;
    private BigDecimal bid;
    private int bidSize;
    private BigDecimal price;
    
    private int lastTradeSize;
    private String lastTradeDateStr;
    private String lastTradeTimeStr;
    private Calendar lastTradeTime;
    
    private BigDecimal open;
    private BigDecimal previousClose;
    private BigDecimal dayLow;
    private BigDecimal dayHigh;
    
    private BigDecimal yearLow;
    private BigDecimal yearHigh;
    private BigDecimal priceAvg50;
    private BigDecimal priceAvg200;
    
    private long volume;
    private long avgVolume;
    
    public StockQuote() {}
    
    public StockQuote(String symbol) {
        this.symbol = symbol;
    }

    public StockQuote(String symbol, TimeZone timeZone, BigDecimal ask, int askSize, BigDecimal bid, int bidSize,
			BigDecimal price, int lastTradeSize, String lastTradeDateStr, String lastTradeTimeStr,
			Calendar lastTradeTime, BigDecimal open, BigDecimal previousClose, BigDecimal dayLow, BigDecimal dayHigh,
			BigDecimal yearLow, BigDecimal yearHigh, BigDecimal priceAvg50, BigDecimal priceAvg200, long volume,
			long avgVolume) {
		super();
		this.symbol = symbol;
		this.timeZone = timeZone;
		this.ask = ask;
		this.askSize = askSize;
		this.bid = bid;
		this.bidSize = bidSize;
		this.price = price;
		this.lastTradeSize = lastTradeSize;
		this.lastTradeDateStr = lastTradeDateStr;
		this.lastTradeTimeStr = lastTradeTimeStr;
		this.lastTradeTime = lastTradeTime;
		this.open = open;
		this.previousClose = previousClose;
		this.dayLow = dayLow;
		this.dayHigh = dayHigh;
		this.yearLow = yearLow;
		this.yearHigh = yearHigh;
		this.priceAvg50 = priceAvg50;
		this.priceAvg200 = priceAvg200;
		this.volume = volume;
		this.avgVolume = avgVolume;
	}

	/**
     * 
     * @return      difference between current price and previous close
     */
    public BigDecimal getChange() {
        return this.price.subtract(this.previousClose);
    }
    
    /**
     * 
     * @return      change relative to previous close
     */
    public BigDecimal getChangeInPercent() {
        return Utils.getPercent(this.getChange(), this.previousClose);
    }
    
    /**
     * 
     * @return      difference between current price and year low
     */
    public BigDecimal getChangeFromYearLow() {
        return this.price.subtract(this.yearLow);
    }
    
    /**
     * 
     * @return      change from year low relative to year low
     */
    public BigDecimal getChangeFromYearLowInPercent() {
        return Utils.getPercent(this.getChangeFromYearLow(), this.yearLow);
    }
    
    /**
     * 
     * @return      difference between current price and year high
     */
    public BigDecimal getChangeFromYearHigh() {
        return this.price.subtract(this.yearHigh);
    }
    
    /**
     * 
     * @return      change from year high relative to year high
     */
    public BigDecimal getChangeFromYearHighInPercent() {
        return Utils.getPercent(this.getChangeFromYearHigh(), this.yearHigh);
    }
    
    /**
     * 
     * @return      difference between current price and 50 day moving average
     */
    public BigDecimal getChangeFromAvg50() {
        return this.price.subtract(this.priceAvg50);
    }
    
    /**
     * 
     * @return      change from 50 day moving average relative to 50 day moving average
     */
    public BigDecimal getChangeFromAvg50InPercent() {
        return Utils.getPercent(this.getChangeFromAvg50(), this.priceAvg50);
    }
    
    /**
     * 
     * @return      difference between current price and 200 day moving average
     */
    public BigDecimal getChangeFromAvg200() {
        return this.price.subtract(this.priceAvg200);
    }
    
    /**
     * 
     * @return      change from 200 day moving average relative to 200 day moving average
     */
    public BigDecimal getChangeFromAvg200InPercent() {
        return Utils.getPercent(this.getChangeFromAvg200(), this.priceAvg200);
    }
    
    public String getSymbol() {
        return symbol;
    }
    
    public BigDecimal getAsk() {
        return ask;
    }
    
    public void setAsk(BigDecimal ask) {
        this.ask = ask;
    }
    
    public int getAskSize() {
        return askSize;
    }
    
    public void setAskSize(int askSize) {
        this.askSize = askSize;
    }
    
    public BigDecimal getBid() {
        return bid;
    }
    
    public void setBid(BigDecimal bid) {
        this.bid = bid;
    }
    
    public int getBidSize() {
        return bidSize;
    }
    
    public void setBidSize(int bidSize) {
        this.bidSize = bidSize;
    }
    
    public BigDecimal getPrice() {
        return price;
    }
    
    public void setPrice(BigDecimal price) {
        this.price = price;
    }
    
    public int getLastTradeSize() {
        return lastTradeSize;
    }
    
    public void setLastTradeSize(int lastTradeSize) {
        this.lastTradeSize = lastTradeSize;
    }

    public String getLastTradeDateStr() {
        return lastTradeDateStr;
    }

    public void setLastTradeDateStr(String lastTradeDateStr) {
        this.lastTradeDateStr = lastTradeDateStr;
    }

    public String getLastTradeTimeStr() {
        return lastTradeTimeStr;
    }

    public void setLastTradeTimeStr(String lastTradeTimeStr) {
        this.lastTradeTimeStr = lastTradeTimeStr;
    }
    
    /**
     * Will derive the time zone from the exchange to parse the date time into a Calendar object.
     * This will not react to changes in the lastTradeDateStr and lastTradeTimeStr
     * 
     * @return last trade date time
     */
    public Calendar getLastTradeTime() {
        return lastTradeTime;
    }
    
    public void setLastTradeTime(Calendar lastTradeTime) {
        this.lastTradeTime = lastTradeTime;
    }
    
    /**
     * Will use the provided time zone to parse the date time into a Calendar object
     * Reacts to changes in the lastTradeDateStr and lastTradeTimeStr
     * 
     * @param timeZone time zone where the stock is traded
     * @return last trade date time
     */
    public Calendar getLastTradeTime(TimeZone timeZone) {
        return Utils.parseDateTime(this.lastTradeDateStr, this.lastTradeTimeStr, timeZone);
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
    
    public BigDecimal getOpen() {
        return open;
    }
    
    public void setOpen(BigDecimal open) {
        this.open = open;
    }
    
    public BigDecimal getPreviousClose() {
        return previousClose;
    }
    
    public void setPreviousClose(BigDecimal previousClose) {
        this.previousClose = previousClose;
    }
    
    public BigDecimal getDayLow() {
        return dayLow;
    }
    
    public void setDayLow(BigDecimal dayLow) {
        this.dayLow = dayLow;
    }
    
    public BigDecimal getDayHigh() {
        return dayHigh;
    }
    
    public void setDayHigh(BigDecimal dayHigh) {
        this.dayHigh = dayHigh;
    }
    
    public BigDecimal getYearLow() {
        return yearLow;
    }
    
    public void setYearLow(BigDecimal yearLow) {
        this.yearLow = yearLow;
    }
    
    public BigDecimal getYearHigh() {
        return yearHigh;
    }
    
    public void setYearHigh(BigDecimal yearHigh) {
        this.yearHigh = yearHigh;
    }
    
    /**
     * 
     * @return      50 day moving average
     */
    public BigDecimal getPriceAvg50() {
        return priceAvg50;
    }
    
    public void setPriceAvg50(BigDecimal priceAvg50) {
        this.priceAvg50 = priceAvg50;
    }
    
    /**
     * 
     * @return      200 day moving average
     */
    public BigDecimal getPriceAvg200() {
        return priceAvg200;
    }
    
    public void setPriceAvg200(BigDecimal priceAvg200) {
        this.priceAvg200 = priceAvg200;
    }
    
    public long getVolume() {
        return volume;
    }
    
    public void setVolume(long volume) {
        this.volume = volume;
    }
    
    public long getAvgVolume() {
        return avgVolume;
    }
    
    public void setAvgVolume(long avgVolume) {
        this.avgVolume = avgVolume;
    }
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ask == null) ? 0 : ask.hashCode());
		result = prime * result + askSize;
		result = prime * result + (int) (avgVolume ^ (avgVolume >>> 32));
		result = prime * result + ((bid == null) ? 0 : bid.hashCode());
		result = prime * result + bidSize;
		result = prime * result + ((dayHigh == null) ? 0 : dayHigh.hashCode());
		result = prime * result + ((dayLow == null) ? 0 : dayLow.hashCode());
		result = prime * result + ((lastTradeDateStr == null) ? 0 : lastTradeDateStr.hashCode());
		result = prime * result + lastTradeSize;
		result = prime * result + ((lastTradeTime == null) ? 0 : lastTradeTime.hashCode());
		result = prime * result + ((lastTradeTimeStr == null) ? 0 : lastTradeTimeStr.hashCode());
		result = prime * result + ((open == null) ? 0 : open.hashCode());
		result = prime * result + ((previousClose == null) ? 0 : previousClose.hashCode());
		result = prime * result + ((price == null) ? 0 : price.hashCode());
		result = prime * result + ((priceAvg200 == null) ? 0 : priceAvg200.hashCode());
		result = prime * result + ((priceAvg50 == null) ? 0 : priceAvg50.hashCode());
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
		result = prime * result + ((timeZone == null) ? 0 : timeZone.hashCode());
		result = prime * result + (int) (volume ^ (volume >>> 32));
		result = prime * result + ((yearHigh == null) ? 0 : yearHigh.hashCode());
		result = prime * result + ((yearLow == null) ? 0 : yearLow.hashCode());
		return result;
	}

    // n.b. timezone doesn't redeifne equals
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StockQuote other = (StockQuote) obj;
		if (ask == null) {
			if (other.ask != null)
				return false;
		} else if (!ask.equals(other.ask))
			return false;
		if (askSize != other.askSize)
			return false;
		if (avgVolume != other.avgVolume)
			return false;
		if (bid == null) {
			if (other.bid != null)
				return false;
		} else if (!bid.equals(other.bid))
			return false;
		if (bidSize != other.bidSize)
			return false;
		if (dayHigh == null) {
			if (other.dayHigh != null)
				return false;
		} else if (!dayHigh.equals(other.dayHigh))
			return false;
		if (dayLow == null) {
			if (other.dayLow != null)
				return false;
		} else if (!dayLow.equals(other.dayLow))
			return false;
		if (lastTradeDateStr == null) {
			if (other.lastTradeDateStr != null)
				return false;
		} else if (!lastTradeDateStr.equals(other.lastTradeDateStr))
			return false;
		if (lastTradeSize != other.lastTradeSize)
			return false;
		if (lastTradeTime == null) {
			if (other.lastTradeTime != null)
				return false;
		} else if (!lastTradeTime.equals(other.lastTradeTime))
			return false;
		if (lastTradeTimeStr == null) {
			if (other.lastTradeTimeStr != null)
				return false;
		} else if (!lastTradeTimeStr.equals(other.lastTradeTimeStr))
			return false;
		if (open == null) {
			if (other.open != null)
				return false;
		} else if (!open.equals(other.open))
			return false;
		if (previousClose == null) {
			if (other.previousClose != null)
				return false;
		} else if (!previousClose.equals(other.previousClose))
			return false;
		if (price == null) {
			if (other.price != null)
				return false;
		} else if (!price.equals(other.price))
			return false;
		if (priceAvg200 == null) {
			if (other.priceAvg200 != null)
				return false;
		} else if (!priceAvg200.equals(other.priceAvg200))
			return false;
		if (priceAvg50 == null) {
			if (other.priceAvg50 != null)
				return false;
		} else if (!priceAvg50.equals(other.priceAvg50))
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		if (timeZone == null) {
			if (other.timeZone != null)
				return false;
		} else if (!timeZone.equals(other.timeZone))
			return false;
		if (volume != other.volume)
			return false;
		if (yearHigh == null) {
			if (other.yearHigh != null)
				return false;
		} else if (!yearHigh.equals(other.yearHigh))
			return false;
		if (yearLow == null) {
			if (other.yearLow != null)
				return false;
		} else if (!yearLow.equals(other.yearLow))
			return false;
		return true;
	}

	@Override
    public String toString() {
        return "Ask: " + this.ask + ", Bid: " + this.bid + ", Price: " + this.price + ", Prev close: " + this.previousClose;
    }
    
}

