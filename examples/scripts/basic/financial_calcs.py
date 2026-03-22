import numpy as np
import numpy_financial as npf
from typing import Dict, List, Optional, Union

class FinancialCalcs:
    """Pure financial calculations — backend agnostic."""

    # === Growth ===
    @staticmethod
    def cagr(start_val: float, end_val: float, years: int) -> Optional[float]:
        """Calculate Compound Annual Growth Rate."""
        if years <= 0 or start_val <= 0 or end_val <= 0:
            return None
        return ((end_val / start_val) ** (1 / years) - 1) * 100

    @staticmethod
    def cagr_series(values_by_year: Dict[int, float], current_year: int, periods: List[int] = [1, 5, 10]) -> Dict[str, Optional[float]]:
        """Calculate CAGR for multiple periods from a dictionary of {year: value}."""
        results = {}
        for period in periods:
            start_year = current_year - period
            if current_year in values_by_year and start_year in values_by_year:
                results[f"{period}Y"] = FinancialCalcs.cagr(values_by_year[start_year], values_by_year[current_year], period)
            else:
                results[f"{period}Y"] = None
        return results

    @staticmethod
    def ttm_growth(recent_4q: Union[List[float], np.ndarray], prev_4q: Union[List[float], np.ndarray]) -> Optional[float]:
        """Calculate YoY TTM Growth: (Sum(Last 4 Q) / Sum(Prev 4 Q)) - 1"""
        sum_recent = np.sum(recent_4q)
        sum_prev = np.sum(prev_4q)
        if sum_prev <= 0:
            return None
        return ((sum_recent / sum_prev) - 1) * 100

    @staticmethod
    def yoy_growth(current_val: float, prior_val: float) -> Optional[float]:
        """Calculate Year-over-Year Growth."""
        if prior_val <= 0:
            return None
        return ((current_val / prior_val) - 1) * 100

    # === Profitability ===
    @staticmethod
    def roic(operating_income: float, tax_rate: float, invested_capital: float) -> Optional[float]:
        """Calculate ROIC (Return on Invested Capital): NOPAT / Invested Capital."""
        nopat = operating_income * (1 - tax_rate)
        if invested_capital == 0:
            return None
        return (nopat / invested_capital) * 100

    @staticmethod
    def invested_capital_gurufocus(
        total_assets: float,
        accounts_payable: float,
        accrued_expense: float,
        cash_and_marketable_securities: float,
        total_current_assets: float,
        total_current_liabilities: float
    ) -> float:
        """
        GuruFocus definition of Invested Capital.
        Invested Capital = Total Assets 
                        - Accounts Payable & Accrued Expense 
                        - Excess Cash
        
        Excess Cash = Cash & Equiv & Marketable Securities 
                    - max(0, Current Liabilities - Current Assets + Cash & Equiv & Marketable Securities)
        """
        excess_cash_offset = max(0, total_current_liabilities - total_current_assets + cash_and_marketable_securities)
        excess_cash = cash_and_marketable_securities - excess_cash_offset
        
        return total_assets - (accounts_payable + accrued_expense) - excess_cash

    @staticmethod
    def roe(net_income: float, equity: float) -> Optional[float]:
        """Calculate Return on Equity."""
        if equity == 0:
            return None
        return (net_income / equity) * 100

    @staticmethod
    def sgr(roe_decimal: float, payout_ratio: float) -> float:
        """Calculate Sustainable Growth Rate: ROE * (1 - Payout Ratio)"""
        return roe_decimal * (1 - payout_ratio) * 100

    # === Valuation ===
    @staticmethod
    def sticker_price(eps: float, growth_rate_pct: float, future_pe: float, marr: float = 0.15, years: int = 10) -> Dict:
        """Phil Town's Sticker Price and Margin of Safety."""
        # 1. Future EPS
        future_eps = eps * ((1 + (growth_rate_pct / 100)) ** years)
        # 2. Future Value
        future_value = future_eps * future_pe
        # 3. Sticker Price (Discount back at MARR)
        sticker_price = future_value / ((1 + marr) ** years)
        # 4. MOS Price (50% margin)
        mos_price = sticker_price * 0.5
        
        return {
            "future_value": future_value,
            "sticker_price": sticker_price,
            "mos_price": mos_price
        }

    @staticmethod
    def dcf_valuation(
        base_fcf: float, 
        growth_rate_pct: float, 
        discount_rate: float, 
        terminal_growth: float,
        shares: float, 
        cash: float, 
        debt: float, 
        years: int = 10
    ) -> Dict:
        """Discounted Cash Flow Valuation."""
        # Project FCFs
        growth_multiplier = 1 + (growth_rate_pct / 100)
        projected_fcfs = [base_fcf * (growth_multiplier ** i) for i in range(1, years + 1)]
        
        # PV of projected FCFs
        pv_fcf_sum = npf.npv(discount_rate, [0] + projected_fcfs)
        
        # Terminal Value (Gordon Growth)
        fcf_final = projected_fcfs[-1]
        tv_growth_val = fcf_final * (1 + terminal_growth) / (discount_rate - terminal_growth)
        pv_terminal_growth = tv_growth_val / ((1 + discount_rate) ** years)
        
        # Enterprise Value
        ev = pv_fcf_sum + pv_terminal_growth
        # Equity Value
        equity_val = ev + cash - debt
        # Per Share
        val_per_share = equity_val / shares if shares > 0 else 0
        
        return {
            "pv_fcf_sum": pv_fcf_sum,
            "pv_terminal": pv_terminal_growth,
            "enterprise_value": ev,
            "equity_value": equity_val,
            "value_per_share": val_per_share
        }

    # === Formatting ===
    @staticmethod
    def format_pct(value: Optional[float], show_sign: bool = True) -> str:
        if value is None:
            return "N/A"
        sign = "+" if show_sign and value > 0 else ""
        return f"{sign}{value:.2f}%"

    @staticmethod
    def format_usd(value: Optional[float], include_usd: bool = True) -> str:
        if value is None:
            return "N/A"
        
        abs_val = abs(value)
        sign = "-" if value < 0 else ""
        prefix = "$" if include_usd else ""
        
        if abs_val >= 1e12:
            return f"{sign}{prefix}{abs_val/1e12:.2f}T"
        elif abs_val >= 1e9:
            return f"{sign}{prefix}{abs_val/1e9:.2f}B"
        elif abs_val >= 1e6:
            return f"{sign}{prefix}{abs_val/1e6:.2f}M"
        else:
            return f"{sign}{prefix}{abs_val:,.0f}"
