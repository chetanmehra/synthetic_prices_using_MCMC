import pandas as pd
import numpy as np


def create_heuristic_regimes(fut, impvol, thresh):
    full_df = pd.merge(fut, impvol, how="left", on=["date"]).ffill()
    full_df["regime"] = np.NaN
    for i, tpl in enumerate(thresh):
        full_df.loc[(full_df["imp_vol"] >= tpl[0])
                    & (full_df["imp_vol"] <= tpl[1]),
                    "regime"] = i - int(len(thresh)/2)
    return full_df


def create_regimes(etf_file, regime_file, regime_thresh, half_life=10):
    etf_df = pd.read_csv(etf_file)
    impvol_df = pd.read_csv(regime_file)
    impvol_df = impvol_df.rename(columns={"VIX Close": "imp_vol"})
    impvol_ema_df = impvol_df["imp_vol"].ewm(halflife=half_life).mean().to_frame()
    impvol_ema_df["date"] = impvol_df["date"]
    etf_df["date"], impvol_df["date"], impvol_ema_df["date"] = pd.to_datetime(etf_df["date"]), \
                                                               pd.to_datetime(impvol_df["date"]), pd.to_datetime(
        impvol_ema_df["date"])

    regime_df = create_heuristic_regimes(etf_df, impvol_ema_df, regime_thresh)
    regime_df["returns"] = regime_df["Adj_Close"].pct_change(1).fillna(0)

    regime_df_agg = regime_df.groupby("regime")
    regime_list, index_start, regime_name = [], 100, []
    for key, val in regime_df_agg:
        regime_ret_index = index_start * (1 + val["returns"]).cumprod()
        stock_regime_df = pd.DataFrame({"date": val["date"], "Adj_Close": regime_ret_index, "regime": key})
        regime_list.append(stock_regime_df)
        regime_name.append(key)

    for regime_idx, regime_name in zip(regime_list, regime_name):
        regime_idx.to_csv(
            f"./SPY_regime_{regime_name}_ewm.csv")


if __name__ == '__main__':
    etf_file = "./SPY_2018.csv"
    vix_file = "./vixcurrent.csv"
    regime_thresh = [(0, 25), (25, 100)]
    create_regimes(etf_file, vix_file, regime_thresh, half_life=10)