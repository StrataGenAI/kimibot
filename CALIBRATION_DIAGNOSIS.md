# KimiBot Calibration Diagnosis

**Date**: 2026-04-21

---

## Root Cause Chain

### 1. Class Imbalance in Training Data

Training markets m1–m14 have the following label distribution:

| Outcome | Markets | Count |
|---------|---------|-------|
| YES     | m2, m8, m12 | 3 (21%) |
| NO      | m1, m3–m7, m9–m11, m13, m14 | 11 (79%) |

This 79% NO imbalance causes `LogisticRegressionModel` (unweighted gradient descent) to minimise loss by pushing all predicted probabilities toward 0. The model learns "predict NO always" as a near-optimal strategy under cross-entropy loss with no class weighting.

### 2. Aggressive Prediction Clipping

`predict_raw()` in `models/predictor.py` clips output:

```python
probability = float(np.clip(probability, 0.20, 0.80))   # line 51
```

With raw model outputs near 0 due to class imbalance, every prediction clips to the lower bound: **0.20**.

### 3. Calibrator Fit on a Single Market

The sigmoid calibrator (Platt scaling) is fit on calibration market m15 only. Platt scaling estimates two parameters (slope `a`, intercept `b`) for:

```
p_calibrated = 1 / (1 + exp(a * raw_score + b))
```

With only 1 market providing calibration samples, the optimisation surface is nearly flat and the resulting parameters are arbitrary. The calibrator effectively passes through or inverts the 0.20 constant input, yielding a single calibrated output near 0.20.

The calibrated output is then clipped again:

```python
calibrated = float(np.clip(calibrated, 0.20, 0.80))    # line 64
```

### 4. Phantom Edge Calculation

With `p_model_calibrated ≈ 0.20` and typical synthetic market prices around 0.45:

```
edge = p_model_calibrated - p_market
     = 0.20 - 0.45
     = −0.25
```

Edge threshold for BUY_NO is `abs(edge) > 0.005`. Every market clears this threshold and receives a BUY NO signal with a phantom 25–36pp edge.

These edges are **not real alpha**. They reflect the model's degenerate constant output, not genuine mispricing in the market.

---

## What Good Calibration Looks Like

After fixes, the expected distribution of predicted probabilities should:
- Span the full `[0.05, 0.95]` range, not cluster at one bound
- Show YES-leaning predictions (~0.65–0.85) for markets that resolve YES
- Brier score < 0.25 on a balanced held-out evaluation set

---

## Fixes Applied (Step 4)

1. **`models/simple_ml.py`**: Balanced class weighting in `LogisticRegressionModel.fit()` — per-sample weights inversely proportional to class frequency.

2. **`models/predictor.py`**: Clip bounds relaxed from `[0.20, 0.80]` → `[0.05, 0.95]` on both raw and calibrated outputs.

3. **`config/default.yaml`**: `min_calibration_markets` raised from 1 to 3.

4. **`backtest/engine.py`**: Post-loop force-settlement so that calibrated predictions actually receive settlement feedback and PnL is computed.

---

## Architectural Notes

The overall calibration architecture is correct:
- Training fold: m1–m14 (fit model weights)
- Calibration fold: m15–m16 (fit Platt sigmoid on held-out raw scores)
- Test fold: m17–m20 (evaluate on unseen markets)

No label leakage was found. The calibration split correctly uses markets not seen during training. The sole problems are dataset size (too few calibration samples) and class imbalance (driving the model to a degenerate constant output before calibration ever runs).
