package it.himyd.finance.yahoo;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.himyd.finance.yahoo.histquotes.HistQuotesRequest;
import it.himyd.finance.yahoo.histquotes.HistoricalQuote;
import it.himyd.finance.yahoo.histquotes.Interval;
import it.himyd.finance.yahoo.quotes.stock.*;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Stock implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String symbol; // was final
    private String name;
    private String currency;
    private String stockExchange;
    
    private StockQuote quote;
    private StockStats stats;
    private StockDividend dividend;
    
    private List<HistoricalQuote> history;
    
    public Stock() {
    }
    
    public Stock(String symbol) {
        this.symbol = symbol;
    }

    public Stock(String symbol, String name, String currency, String stockExchange, StockQuote quote, StockStats stats,
			StockDividend dividend, List<HistoricalQuote> history) {
		super();
		this.symbol = symbol;
		this.name = name;
		this.currency = currency;
		this.stockExchange = stockExchange;
		this.quote = quote;
		this.stats = stats;
		this.dividend = dividend;
		this.history = history;
	}
    
    
    private void update() throws IOException {
        StockQuotesRequest request = new StockQuotesRequest(this.symbol);
        StockQuotesData data = request.getSingleResult();
        if(data != null) {
            this.setQuote(data.getQuote());
            this.setStats(data.getStats());
            this.setDividend(data.getDividend());
            YahooFinance.logger.log(Level.INFO, "Updated Stock with symbol: {0}", this.symbol);
        } else {
            YahooFinance.logger.log(Level.SEVERE, "Failed to update Stock with symbol: {0}", this.symbol);
        }
    }
    
	/**
     * Returns the basic quotes data available for this stock.
     * 
     * @return      basic quotes data available for this stock
     * @see         #getQuote(boolean) 
     */
    public StockQuote getQuote() {
        return this.quote;
    }
    
    /**
     * Returns the basic quotes data available for this stock.
     * This method will return null in the following situations:
     * <ul>
     * <li> the data hasn't been loaded yet
     *      in a previous request and refresh is set to false.
     * <li> refresh is true and the data cannot be retrieved from Yahoo Finance 
     *      for whatever reason (symbol not recognized, no network connection, ...)
     * </ul>
     * <p>
     * When the quote data gets refreshed, it will automatically also refresh
     * the statistics and dividend data of the stock from Yahoo Finance 
     * in the same request.
     * 
     * @param refresh   indicates whether the data should be requested again to Yahoo Finance
     * @return          basic quotes data available for this stock
     * @throws IOException when there's a connection problem
     */
    public StockQuote getQuote(boolean refresh) throws IOException {
        if(refresh) {
            this.update();
        }
        return this.quote;
    }
    
    public void setQuote(StockQuote quote) {
        this.quote = quote;
    }
    
    /**
     * Returns the statistics available for this stock.
     * 
     * @return      statistics available for this stock
     * @see         #getStats(boolean) 
     */
    public StockStats getStats() {
        return this.stats;
    }
    
    /**
     * Returns the statistics available for this stock.
     * This method will return null in the following situations:
     * <ul>
     * <li> the data hasn't been loaded yet
     *      in a previous request and refresh is set to false.
     * <li> refresh is true and the data cannot be retrieved from Yahoo Finance 
     *      for whatever reason (symbol not recognized, no network connection, ...)
     * </ul>
     * <p>
     * When the statistics get refreshed, it will automatically also refresh
     * the quote and dividend data of the stock from Yahoo Finance 
     * in the same request.
     * 
     * @param refresh   indicates whether the data should be requested again to Yahoo Finance
     * @return          statistics available for this stock
     * @throws IOException when there's a connection problem
     */
    public StockStats getStats(boolean refresh) throws IOException {
        if(refresh) {
            this.update();
        }
        return this.stats;
    }
    
    public void setStats(StockStats stats) {
        this.stats = stats;
    }
    
    /**
     * Returns the dividend data available for this stock.
     * 
     * @return      dividend data available for this stock
     * @see         #getDividend(boolean) 
     */
    public StockDividend getDividend() {
        return this.dividend;
    }
    
    /**
     * Returns the dividend data available for this stock.
     * 
     * This method will return null in the following situations:
     * <ul>
     * <li> the data hasn't been loaded yet
     *      in a previous request and refresh is set to false.
     * <li> refresh is true and the data cannot be retrieved from Yahoo Finance 
     *      for whatever reason (symbol not recognized, no network connection, ...)
     * </ul>
     * <p>
     * When the dividend data get refreshed, it will automatically also refresh
     * the quote and statistics data of the stock from Yahoo Finance 
     * in the same request.
     * 
     * @param refresh   indicates whether the data should be requested again to Yahoo Finance
     * @return          dividend data available for this stock
     * @throws IOException when there's a connection problem
     */
    public StockDividend getDividend(boolean refresh) throws IOException {
        if(refresh) {
            this.update();
        }
        return this.dividend;
    }
    
    public void setDividend(StockDividend dividend) {
        this.dividend = dividend;
    }
    
    /**
     * This method will return historical quotes from this stock.
     * If the historical quotes are not available yet, they will 
     * be requested first from Yahoo Finance.
     * <p>
     * If the historical quotes are not available yet, the
     * following characteristics will be used for the request:
     * <ul>
     * <li> from: 1 year ago (default)
     * <li> to: today (default)
     * <li> interval: MONTHLY (default)
     * </ul>
     * <p>
     * There are several more methods available that allow you
     * to define some characteristics of the historical data.
     * Calling one of those methods will result in a new request
     * being sent to Yahoo Finance.
     * 
     * @return      a list of historical quotes from this stock
     * @throws IOException when there's a connection problem
     * @see         #getHistory(Interval)
     * @see         #getHistory(Calendar)
     * @see         #getHistory(Calendar, Calendar)
     * @see         #getHistory(Calendar, Interval)
     * @see         #getHistory(Calendar, Calendar, Interval)
     */
    public List<HistoricalQuote> getHistory() throws IOException {
        if(this.history != null) {
            return this.history;
        }
        return this.getHistory(HistQuotesRequest.DEFAULT_FROM);
    }
    
    /**
     * Requests the historical quotes for this stock with the following characteristics.
     * <ul>
     * <li> from: 1 year ago (default)
     * <li> to: today (default)
     * <li> interval: specified value
     * </ul>
     * 
     * @param interval      the interval of the historical data
     * @return              a list of historical quotes from this stock
     * @throws IOException when there's a connection problem
     * @see                 #getHistory() 
     */
    public List<HistoricalQuote> getHistory(Interval interval) throws IOException {
        return this.getHistory(HistQuotesRequest.DEFAULT_FROM, interval);
    }
    
    /**
     * Requests the historical quotes for this stock with the following characteristics.
     * <ul>
     * <li> from: specified value
     * <li> to: today (default)
     * <li> interval: MONTHLY (default)
     * </ul>
     * 
     * @param from          start date of the historical data
     * @return              a list of historical quotes from this stock
     * @throws IOException when there's a connection problem
     * @see                 #getHistory() 
     */
    public List<HistoricalQuote> getHistory(Calendar from) throws IOException {
        return this.getHistory(from, HistQuotesRequest.DEFAULT_TO);
    }
    
    /**
     * Requests the historical quotes for this stock with the following characteristics.
     * <ul>
     * <li> from: specified value
     * <li> to: today (default)
     * <li> interval: specified value
     * </ul>
     * 
     * @param from          start date of the historical data
     * @param interval      the interval of the historical data
     * @return              a list of historical quotes from this stock
     * @throws IOException when there's a connection problem
     * @see                 #getHistory() 
     */
    public List<HistoricalQuote> getHistory(Calendar from, Interval interval) throws IOException {
        return this.getHistory(from, HistQuotesRequest.DEFAULT_TO, interval);
    }
    
    /**
     * Requests the historical quotes for this stock with the following characteristics.
     * <ul>
     * <li> from: specified value
     * <li> to: specified value
     * <li> interval: MONTHLY (default)
     * </ul>
     * 
     * @param from          start date of the historical data
     * @param to            end date of the historical data
     * @return              a list of historical quotes from this stock
     * @throws IOException when there's a connection problem
     * @see                 #getHistory() 
     */
    public List<HistoricalQuote> getHistory(Calendar from, Calendar to) throws IOException {
        return this.getHistory(from, to, Interval.MONTHLY);
    }
    
    /**
     * Requests the historical quotes for this stock with the following characteristics.
     * <ul>
     * <li> from: specified value
     * <li> to: specified value
     * <li> interval: specified value
     * </ul>
     * 
     * @param from          start date of the historical data
     * @param to            end date of the historical data
     * @param interval      the interval of the historical data
     * @return              a list of historical quotes from this stock
     * @throws IOException when there's a connection problem
     * @see                 #getHistory() 
     */
    public List<HistoricalQuote> getHistory(Calendar from, Calendar to, Interval interval) throws IOException {
        HistQuotesRequest hist = new HistQuotesRequest(this.symbol, from, to, interval);
        this.setHistory(hist.getResult());
        return this.history;
    }
    
    public void setHistory(List<HistoricalQuote> history) {
        this.history = history;
    }
    
    public String getSymbol() {
        return symbol;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getCurrency() {
        return currency;
    }
    
    public void setCurrency(String currency) {
        this.currency = currency;
    }
    
    public String getStockExchange() {
        return stockExchange;
    }
    
    public void setStockExchange(String stockExchange) {
        this.stockExchange = stockExchange;
    }
    
       
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((currency == null) ? 0 : currency.hashCode());
		result = prime * result + ((dividend == null) ? 0 : dividend.hashCode());
		result = prime * result + ((history == null) ? 0 : history.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((quote == null) ? 0 : quote.hashCode());
		result = prime * result + ((stats == null) ? 0 : stats.hashCode());
		result = prime * result + ((stockExchange == null) ? 0 : stockExchange.hashCode());
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
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
		Stock other = (Stock) obj;
		if (currency == null) {
			if (other.currency != null)
				return false;
		} else if (!currency.equals(other.currency))
			return false;
		if (dividend == null) {
			if (other.dividend != null)
				return false;
		} else if (!dividend.equals(other.dividend))
			return false;
		if (history == null) {
			if (other.history != null)
				return false;
		} else if (!history.equals(other.history))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (quote == null) {
			if (other.quote != null)
				return false;
		} else if (!quote.equals(other.quote))
			return false;
		if (stats == null) {
			if (other.stats != null)
				return false;
		} else if (!stats.equals(other.stats))
			return false;
		if (stockExchange == null) {
			if (other.stockExchange != null)
				return false;
		} else if (!stockExchange.equals(other.stockExchange))
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		return true;
	}

	@Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(this.symbol);
    	sb.append(" | ");
    	sb.append(this.name);
    	sb.append(" | ");
    	sb.append(this.quote.toString());
        return sb.toString();
    }
    
    public String details() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(this.symbol + ":");
    	sb.append(this.quote.getPrice());
//    	sb.append(this.quote.toString());
        return sb.toString();
    }
    
    public void print() {
        System.out.println(this.symbol);
        System.out.println("--------------------------------");
        for (Field f : this.getClass().getDeclaredFields()) {
            try {
                System.out.println(f.getName() + ": " + f.get(this));
            } catch (IllegalArgumentException ex) {
                Logger.getLogger(Stock.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Stock.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("--------------------------------");
    }
    
    public String toJSONString() {
    	ObjectMapper mapper = new ObjectMapper();
    	String jsonInString = null;
		try {
			jsonInString = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return jsonInString;
    }
    
    public static Stock fromJSONString(String jsonString) {
    	ObjectMapper mapper = new ObjectMapper();

    	//JSON from String to Object
    	Stock stock = null;
		try {
			stock = mapper.readValue(jsonString, Stock.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return stock;
    }
    
}
