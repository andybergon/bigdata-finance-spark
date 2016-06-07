
package it.himyd.finance.yahoo.quotes.stock;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Calendar;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class StockDividend implements Serializable {

	private static final long serialVersionUID = 1L;
    
    private String symbol;
    
    private Calendar payDate;
    private Calendar exDate;
    private BigDecimal annualYield;
    private BigDecimal annualYieldPercent;
    
    public StockDividend() {}
    
    public StockDividend(String symbol) {
        this.symbol = symbol;
    }
    
    public StockDividend(String symbol, Calendar payDate, Calendar exDate, BigDecimal annualYield, BigDecimal annualYieldPercent) {
        this(symbol);
        this.payDate = payDate;
        this.exDate = exDate;
        this.annualYield = annualYield;
        this.annualYieldPercent = annualYieldPercent;
    }

    public String getSymbol() {
        return symbol;
    }
    
    public Calendar getPayDate() {
        return payDate;
    }

    public void setPayDate(Calendar payDate) {
        this.payDate = payDate;
    }

    public Calendar getExDate() {
        return exDate;
    }

    public void setExDate(Calendar exDate) {
        this.exDate = exDate;
    }
    
    public BigDecimal getAnnualYield() {
        return annualYield;
    }
    
    public void setAnnualYield(BigDecimal annualYield) {
        this.annualYield = annualYield;
    }
    
    public BigDecimal getAnnualYieldPercent() {
        return annualYieldPercent;
    }
    
    public void setAnnualYieldPercent(BigDecimal annualYieldPercent) {
        this.annualYieldPercent = annualYieldPercent;
    }
    
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((annualYield == null) ? 0 : annualYield.hashCode());
		result = prime * result + ((annualYieldPercent == null) ? 0 : annualYieldPercent.hashCode());
		result = prime * result + ((exDate == null) ? 0 : exDate.hashCode());
		result = prime * result + ((payDate == null) ? 0 : payDate.hashCode());
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
		StockDividend other = (StockDividend) obj;
		if (annualYield == null) {
			if (other.annualYield != null)
				return false;
		} else if (!annualYield.equals(other.annualYield))
			return false;
		if (annualYieldPercent == null) {
			if (other.annualYieldPercent != null)
				return false;
		} else if (!annualYieldPercent.equals(other.annualYieldPercent))
			return false;
		if (exDate == null) {
			if (other.exDate != null)
				return false;
		} else if (!exDate.equals(other.exDate))
			return false;
		if (payDate == null) {
			if (other.payDate != null)
				return false;
		} else if (!payDate.equals(other.payDate))
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
        String payDateStr = "/";
        String exDateStr = "/";
        if(this.payDate != null) {
            payDateStr = this.payDate.getTime().toString();
        }
        if(this.exDate != null) {
            exDateStr = this.exDate.getTime().toString();
        }
        return "Pay date: " + payDateStr + ", Ex date: " + exDateStr + ", Annual yield: " + this.getAnnualYieldPercent() + "%";
    }
    
}
