# HYPOTHESIS.md

*Written before first honest evaluation against real Limitless data. The point is to lock in expectations now so the eventual results actually teach me something instead of being post-rationalized.*

---

## What this model is

A logistic regression forecaster for **short-duration crypto-resolved binary prediction markets on Limitless**. It takes the current market price plus minute-level momentum and underlying BTC/perp signals, and outputs a calibrated probability that the market resolves YES.

Architecture: scikit-learn LogisticRegression → StandardScaler → probability calibrator. Trained walk-forward on real resolved Limitless markets, with BTC price/funding context joined from Binance public API data.

This is NOT a general-purpose forecaster. It is built for one specific market type on one specific platform. Evaluating it on anything else (Manifold, political markets, sports) would not produce meaningful results.

---

## Features the model uses

The 10 features in `FEATURE_COLUMNS`:

| Feature | What it measures | Why it might add information beyond `p_market` alone |
|---|---|---|
| `p_market` | Current Limitless market price | Baseline anchor. Without it, the model has nothing to compare against. With only it, the model can never beat the market. |
| `momentum_1m` | Market price change over last 1 minute | Short-term price movement may reveal direction not yet fully reflected in current price, especially in thinner Limitless markets where price discovery is slower than HFT venues. |
| `momentum_5m` | Market price change over last 5 minutes | Captures sustained directional pressure vs single-print noise. |
| `volatility` | Stdev of last 5 market price observations | High intra-market volatility may indicate disagreement / unresolved information. The model can learn that high-vol markets are systematically more or less reliable. |
| `volume_spike` | Current volume vs trailing 5-period average | Sudden volume spikes often precede price moves; informed flow shows up as size before it shows up as price. |
| `btc_return_1m` | BTC price change over last 1 minute | For BTC-resolved markets, the underlying moves should mechanically affect resolution probability. Hypothesis: thinly-traded prediction markets lag the perp/spot market. |
| `btc_return_5m` | BTC price change over last 5 minutes | Same logic, longer window — captures sustained moves vs single-print noise. |
| `btc_return_15m` | BTC price change over last 15 minutes | Captures meaningful directional moves that should be priced in but might not be. |
| `btc_volatility` | Recent BTC price volatility (5-period) | High BTC vol means resolution is more uncertain in either direction; model can learn to be less confident in high-vol regimes. |
| `funding_rate` | Current BTC perp funding rate | Funding reflects directional positioning of perp traders; persistent positive/negative funding may indicate crowded one-sided bets the market hasn't fully absorbed. **This is the feature I think is most likely to add real edge** because it captures information from a much larger and more efficient market (perps) than Limitless itself. |
| `time_to_resolution` | Seconds until market resolves | Markets close to resolution should converge to 0 or 1; far-from-resolution markets are noisier. The model can learn this convergence pattern. |

---

## My core hypothesis

**Limitless prediction markets price-discover slower than the BTC perp market they reference.** When BTC moves significantly on Binance perps in the last 1–15 minutes, the corresponding Limitless markets should adjust — but because Limitless is thinner and less efficient, there's a lag. The model's job is to detect when this lag is exploitable.

The features I expect to actually carry signal: `funding_rate`, `btc_return_5m`, `btc_return_15m`, `time_to_resolution`. The momentum/volatility features on the prediction market itself are more speculative — they might just be noise.

---

## What Brier score would I consider a positive signal?

The market price baseline is the bar. I will compute Brier of "predict = current `p_market`" on the same TEST set and compare.

| Model Brier vs Market Baseline | My interpretation |
|---|---|
| Worse than baseline | No edge. Model is hurting. Stop and diagnose before doing anything else. |
| Within ±0.005 of baseline | Statistically indistinguishable from copying the market. Need more data or different features. |
| 0.005 to 0.02 better than baseline, on 100+ TEST markets | Potential edge. Worth investigating which features are doing the work and whether it's robust across time windows. |
| 0.02 to 0.05 better than baseline | Strong signal, IF confidence intervals exclude zero. Verify on out-of-time data before getting excited. |
| More than 0.05 better than baseline | Suspicious. Almost certainly look-ahead bias, label leakage, or evaluation bug. Audit before celebrating. |

ECE target: below 0.05. Above 0.10 means the model's stated probabilities don't match reality and I can't use them for sizing decisions even if Brier is decent.

Sample size matters more than the point estimate. A Brier delta of 0.015 on 30 markets is noise; the same delta on 200 markets is signal. Bootstrap confidence intervals will tell the truth here.

---

## If the result is bad, first things to try

| Symptom | First diagnosis step |
|---|---|
| Brier ≥ market baseline AND predictions cluster tightly around `p_market` | Model has learned "just copy the market." Need features the market doesn't already see. The `funding_rate` and BTC-return features might need stronger weighting or transformation. |
| Brier ≥ market baseline AND predictions are wildly different from `p_market` | Model is making confident wrong calls. Likely overfitting on TRAIN. Check regularization (L2 strength), reduce model complexity, or expand training set. |
| Brier slightly better than baseline but only on a small subset | Check whether the edge is real or selection bias. Look at the per-decile Brier breakdown — is the edge concentrated in extreme probabilities (which are rare and high-variance) or spread across the distribution? |
| Brier suspiciously much better than baseline (>0.05 delta) | LOOK-AHEAD AUDIT FIRST. Check that no feature uses data from after `as_of`. Check that the calibrator was fit on CALIBRATE not TRAIN. Check that markets in TEST aren't somehow correlated with TRAIN markets. |
| Model beats baseline on TRAIN/CALIBRATE but not TEST | Overfitting. Reduce features or add regularization. |
| ECE high (>0.10) even when Brier looks decent | Calibrator is broken. Refit on a clean held-out window. Try isotonic if Platt was used or vice versa. |

---

## Things I genuinely don't know yet

- Whether enough resolved binary Limitless markets exist in the last 6–12 months to make any of this statistically meaningful. Need at least 30 in TEST, ideally 100+. If the platform is too young or too sparse, the answer might be "come back in 6 months."
- Whether the `funding_rate` feature, which I'm betting on as the main edge source, is actually orthogonal to `p_market` or whether the market already incorporates it. If markets fully price funding, this whole approach has no edge.
- Whether minute-level granularity is even available consistently from Limitless's data — if their trade history is sparse (e.g. one trade every 30 minutes for a given market), the momentum features will be mostly zero and add no information.
- Whether the model trained on synthetic data has weights that are anywhere near correct for real data, or whether the first real-data training will produce dramatically different weights. The latter is more likely.

---

## Decision rule after first evaluation

- **If model beats market baseline by ≥0.005 Brier with CI excluding zero on ≥100 TEST markets:** Investigate which features drive it. Consider next-step prompt: improve the strongest features, retrain, re-evaluate.
- **If model fails to beat market baseline:** Do NOT rush to add new features or change the model. First understand WHY — read the disagreement diagnostics, look at per-decile breakdown, check whether the failure is uniform or concentrated. Fix the diagnosis before fixing the model.
- **If sample size is too small for confidence:** Either wait for more markets to resolve (weeks/months) or accept that the first signal is preliminary and re-evaluate quarterly.
- **In NO case:** start wiring up live trading until the model has shown stable, statistically meaningful edge across at least two independent evaluation windows.

---

*Committed before first real evaluation. If I edit this file later to make the predictions match the results, I have failed at the entire point of writing it.*
