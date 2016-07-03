package it.himyd.spark.ml.classification.training;

import java.util.Date;

public class OHLCstock {
	String name;
	Date date;
	Double open, high, low, close, volume;

	public OHLCstock(String lineString) {
		super();

		String[] line = lineString.split(",");

		String day = line[1];
		String time = line[2];

		this.name = line[0];
		this.date = stringsToDate(day, time);
		this.open = Double.valueOf(line[3]);
		this.high = Double.valueOf(line[4]);
		this.low = Double.valueOf(line[5]);
		this.close = Double.valueOf(line[6]);
		this.volume = Double.valueOf(line[7]);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
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

	public Double getVolume() {
		return volume;
	}

	public void setVolume(Double volume) {
		this.volume = volume;
	}

	public Date stringsToDate(String day, String time) {
		Date date = new Date();
		String yyyy = day.substring(0, 4);
		String mm = day.substring(4, 6);
		String dd = day.substring(6, 8);

		// System.out.println(Integer.valueOf(yyyy));
		// System.out.println(Integer.valueOf(mm));
		// System.out.println(Integer.valueOf(dd));

		date.setYear(Integer.valueOf(yyyy));
		date.setMonth(Integer.valueOf(mm));
		date.setDate((Integer.valueOf(dd)));

		Integer hour = (Integer.valueOf(time.split(":")[0]));
		Integer minute = (Integer.valueOf(time.split(":")[1]));

		date.setHours(hour);
		date.setMinutes(minute);
		date.setSeconds(0);

		return date;
	}

	@Override
	public String toString() {
		return "OHLCstock [name=" + name + ", date=" + date + ", open=" + open + ", high=" + high + ", low=" + low
				+ ", close=" + close + ", volume=" + volume + "]";
	}

}
