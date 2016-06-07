
package it.himyd.finance.yahoo.quotes.stock;

import java.io.Serializable;
import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import it.himyd.finance.yahoo.Utils;

@JsonIgnoreProperties(ignoreUnknown=true)
public class StockStats implements Serializable {

	private static final long serialVersionUID = 1L;
    
    private String symbol;
    
    private BigDecimal marketCap;
    private long sharesFloat;
    private long sharesOutstanding;
    private long sharesOwned;
    
    private BigDecimal eps;
    private BigDecimal pe;
    private BigDecimal peg;
    
    private BigDecimal epsEstimateCurrentYear;
    private BigDecimal epsEstimateNextQuarter;
    private BigDecimal epsEstimateNextYear;
    
    private BigDecimal priceBook;
    private BigDecimal priceSales;
    private BigDecimal bookValuePerShare;
    
    private BigDecimal revenue; // ttm
    private BigDecimal EBITDA; // ttm
    private BigDecimal oneYearTargetPrice;
    
    public StockStats() {}
    
    public StockStats(String symbol) {
        this.symbol = symbol;
    }
    
    public StockStats(String symbol, BigDecimal marketCap, long sharesFloat, long sharesOutstanding, long sharesOwned,
			BigDecimal eps, BigDecimal pe, BigDecimal peg, BigDecimal epsEstimateCurrentYear,
			BigDecimal epsEstimateNextQuarter, BigDecimal epsEstimateNextYear, BigDecimal priceBook,
			BigDecimal priceSales, BigDecimal bookValuePerShare, BigDecimal revenue, BigDecimal eBITDA,
			BigDecimal oneYearTargetPrice) {
		super();
		this.symbol = symbol;
		this.marketCap = marketCap;
		this.sharesFloat = sharesFloat;
		this.sharesOutstanding = sharesOutstanding;
		this.sharesOwned = sharesOwned;
		this.eps = eps;
		this.pe = pe;
		this.peg = peg;
		this.epsEstimateCurrentYear = epsEstimateCurrentYear;
		this.epsEstimateNextQuarter = epsEstimateNextQuarter;
		this.epsEstimateNextYear = epsEstimateNextYear;
		this.priceBook = priceBook;
		this.priceSales = priceSales;
		this.bookValuePerShare = bookValuePerShare;
		this.revenue = revenue;
		EBITDA = eBITDA;
		this.oneYearTargetPrice = oneYearTargetPrice;
	}

	public BigDecimal getROE() {
        return Utils.getPercent(this.EBITDA, this.marketCap);
    }

    public String getSymbol() {
        return symbol;
    }
    
    public BigDecimal getMarketCap() {
        return marketCap;
    }

    public void setMarketCap(BigDecimal marketCap) {
        this.marketCap = marketCap;
    }

    public long getSharesFloat() {
        return sharesFloat;
    }

    public void setSharesFloat(long sharesFloat) {
        this.sharesFloat = sharesFloat;
    }

    public long getSharesOutstanding() {
        return sharesOutstanding;
    }

    public void setSharesOutstanding(long sharesOutstanding) {
        this.sharesOutstanding = sharesOutstanding;
    }

    public long getSharesOwned() {
        return sharesOwned;
    }

    public void setSharesOwned(long sharesOwned) {
        this.sharesOwned = sharesOwned;
    }

    public BigDecimal getEps() {
        return eps;
    }

    public void setEps(BigDecimal eps) {
        this.eps = eps;
    }

    public BigDecimal getPe() {
        return pe;
    }

    public void setPe(BigDecimal pe) {
        this.pe = pe;
    }

    public BigDecimal getPeg() {
        return peg;
    }

    public void setPeg(BigDecimal peg) {
        this.peg = peg;
    }

    public BigDecimal getEpsEstimateCurrentYear() {
        return epsEstimateCurrentYear;
    }

    public void setEpsEstimateCurrentYear(BigDecimal epsEstimateCurrentYear) {
        this.epsEstimateCurrentYear = epsEstimateCurrentYear;
    }

    public BigDecimal getEpsEstimateNextQuarter() {
        return epsEstimateNextQuarter;
    }

    public void setEpsEstimateNextQuarter(BigDecimal epsEstimateNextQuarter) {
        this.epsEstimateNextQuarter = epsEstimateNextQuarter;
    }

    public BigDecimal getEpsEstimateNextYear() {
        return epsEstimateNextYear;
    }

    public void setEpsEstimateNextYear(BigDecimal epsEstimateNextYear) {
        this.epsEstimateNextYear = epsEstimateNextYear;
    }

    public BigDecimal getPriceBook() {
        return priceBook;
    }

    public void setPriceBook(BigDecimal priceBook) {
        this.priceBook = priceBook;
    }

    public BigDecimal getPriceSales() {
        return priceSales;
    }

    public void setPriceSales(BigDecimal priceSales) {
        this.priceSales = priceSales;
    }

    public BigDecimal getBookValuePerShare() {
        return bookValuePerShare;
    }

    public void setBookValuePerShare(BigDecimal bookValuePerShare) {
        this.bookValuePerShare = bookValuePerShare;
    }

    public BigDecimal getRevenue() {
        return revenue;
    }

    public void setRevenue(BigDecimal revenue) {
        this.revenue = revenue;
    }

    public BigDecimal getEBITDA() {
        return EBITDA;
    }

    public void setEBITDA(BigDecimal EBITDA) {
        this.EBITDA = EBITDA;
    }

    public BigDecimal getOneYearTargetPrice() {
        return oneYearTargetPrice;
    }

    public void setOneYearTargetPrice(BigDecimal oneYearTargetPrice) {
        this.oneYearTargetPrice = oneYearTargetPrice;
    }
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((EBITDA == null) ? 0 : EBITDA.hashCode());
		result = prime * result + ((bookValuePerShare == null) ? 0 : bookValuePerShare.hashCode());
		result = prime * result + ((eps == null) ? 0 : eps.hashCode());
		result = prime * result + ((epsEstimateCurrentYear == null) ? 0 : epsEstimateCurrentYear.hashCode());
		result = prime * result + ((epsEstimateNextQuarter == null) ? 0 : epsEstimateNextQuarter.hashCode());
		result = prime * result + ((epsEstimateNextYear == null) ? 0 : epsEstimateNextYear.hashCode());
		result = prime * result + ((marketCap == null) ? 0 : marketCap.hashCode());
		result = prime * result + ((oneYearTargetPrice == null) ? 0 : oneYearTargetPrice.hashCode());
		result = prime * result + ((pe == null) ? 0 : pe.hashCode());
		result = prime * result + ((peg == null) ? 0 : peg.hashCode());
		result = prime * result + ((priceBook == null) ? 0 : priceBook.hashCode());
		result = prime * result + ((priceSales == null) ? 0 : priceSales.hashCode());
		result = prime * result + ((revenue == null) ? 0 : revenue.hashCode());
		result = prime * result + (int) (sharesFloat ^ (sharesFloat >>> 32));
		result = prime * result + (int) (sharesOutstanding ^ (sharesOutstanding >>> 32));
		result = prime * result + (int) (sharesOwned ^ (sharesOwned >>> 32));
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
		StockStats other = (StockStats) obj;
		if (EBITDA == null) {
			if (other.EBITDA != null)
				return false;
		} else if (!EBITDA.equals(other.EBITDA))
			return false;
		if (bookValuePerShare == null) {
			if (other.bookValuePerShare != null)
				return false;
		} else if (!bookValuePerShare.equals(other.bookValuePerShare))
			return false;
		if (eps == null) {
			if (other.eps != null)
				return false;
		} else if (!eps.equals(other.eps))
			return false;
		if (epsEstimateCurrentYear == null) {
			if (other.epsEstimateCurrentYear != null)
				return false;
		} else if (!epsEstimateCurrentYear.equals(other.epsEstimateCurrentYear))
			return false;
		if (epsEstimateNextQuarter == null) {
			if (other.epsEstimateNextQuarter != null)
				return false;
		} else if (!epsEstimateNextQuarter.equals(other.epsEstimateNextQuarter))
			return false;
		if (epsEstimateNextYear == null) {
			if (other.epsEstimateNextYear != null)
				return false;
		} else if (!epsEstimateNextYear.equals(other.epsEstimateNextYear))
			return false;
		if (marketCap == null) {
			if (other.marketCap != null)
				return false;
		} else if (!marketCap.equals(other.marketCap))
			return false;
		if (oneYearTargetPrice == null) {
			if (other.oneYearTargetPrice != null)
				return false;
		} else if (!oneYearTargetPrice.equals(other.oneYearTargetPrice))
			return false;
		if (pe == null) {
			if (other.pe != null)
				return false;
		} else if (!pe.equals(other.pe))
			return false;
		if (peg == null) {
			if (other.peg != null)
				return false;
		} else if (!peg.equals(other.peg))
			return false;
		if (priceBook == null) {
			if (other.priceBook != null)
				return false;
		} else if (!priceBook.equals(other.priceBook))
			return false;
		if (priceSales == null) {
			if (other.priceSales != null)
				return false;
		} else if (!priceSales.equals(other.priceSales))
			return false;
		if (revenue == null) {
			if (other.revenue != null)
				return false;
		} else if (!revenue.equals(other.revenue))
			return false;
		if (sharesFloat != other.sharesFloat)
			return false;
		if (sharesOutstanding != other.sharesOutstanding)
			return false;
		if (sharesOwned != other.sharesOwned)
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
        return "EPS: " + this.eps + ", PE: " + this.pe + ", PEG: " + this.peg;
    }
    
}
