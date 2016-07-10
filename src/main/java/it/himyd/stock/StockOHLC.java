package it.himyd.stock;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class StockOHLC implements Serializable {
	private static final long serialVersionUID = 1L;

	String symbol;
	Calendar tradetime;
	Double open, high, low, close;
	Long volume;

	public StockOHLC() {
		super();
		this.symbol = new String();
		this.tradetime = Calendar.getInstance();
		this.open = new Double(0);
		this.high = new Double(0);
		this.low = new Double(0);
		this.close = new Double(0);
		this.volume = new Long(0);
	}

	public StockOHLC(String lineString) {
		super();

		String[] line = lineString.split(",");

		String day = line[1];
		String time = line[2];

		this.symbol = line[0];
		this.tradetime = stringsToDate(day, time);
		this.open = Double.valueOf(line[3]);
		this.high = Double.valueOf(line[4]);
		this.low = Double.valueOf(line[5]);
		this.close = Double.valueOf(line[6]);
		this.volume = Long.valueOf(line[7]);
	}

	public StockOHLC(String symbol, Calendar time, Double open, Double high, Double low, Double close, Long volume) {
		super();
		this.symbol = symbol;
		this.tradetime = time;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.volume = volume;
	}

	public Calendar stringsToDate(String day, String time) {
		Date date = new Date();
		String yyyy = day.substring(0, 4);
		String mm = day.substring(4, 6);
		String dd = day.substring(6, 8);

		date.setYear(Integer.valueOf(yyyy) - 1900);
		date.setMonth(Integer.valueOf(mm) - 1);
		date.setDate((Integer.valueOf(dd)));

		Integer hour = (Integer.valueOf(time.split(":")[0]));
		Integer minute = (Integer.valueOf(time.split(":")[1]));

		date.setHours(hour);
		date.setMinutes(minute);
		date.setSeconds(0);

		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);

		return calendar;
	}

	public String toJSONstring() {
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		String json = "";
		try {
			json = ow.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return json;
	}
	
	@Override
	public String toString() {
		return "StockOHLC [symbol=" + symbol + ", time=" + tradetime.getTime() + ", open=" + open + ", high=" + high
				+ ", low=" + low + ", close=" + close + ", volume=" + volume + "]";
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Calendar getTradetime() {
		return tradetime;
	}

	public void setTradetime(Calendar tradetime) {
		this.tradetime = tradetime;
	}

	public Double getOpen() {
		return open;
	}

	public void setOpen(Double open) {
		this.open = open;
	}

	public Double getHigh() {
		return high;
	}

	public void setHigh(Double high) {
		this.high = high;
	}

	public Double getLow() {
		return low;
	}

	public void setLow(Double low) {
		this.low = low;
	}

	public Double getClose() {
		return close;
	}

	public void setClose(Double close) {
		this.close = close;
	}

	public Long getVolume() {
		return volume;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

}
