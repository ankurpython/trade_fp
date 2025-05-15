
#  Trade Similarity & Categorization System

This project analyzes trading data to detect and categorize similar trades between different accounts. It outputs potential copy trades, reverse trades, and partial copies based on configurable behavior rules.

---

## Features

- **Detect similar trades** based on symbol, timing, and account.
- **Classify trades** as:
  - Copy Trade
  - Reverse Trade
  - Partial Copy
-  **Configurable mode** to toggle rule enforcement (e.g., same user violations).
-  **Outputs structured CSVs** for further analysis.

---

## Requirements

- Python 3.8+
- pandas

Install dependencies using pip:

```bash
pip install pandas
```

---

## 📂 File Structure

```
.
├── data/
│   └── test_task_trades_short.csv        # Input data
├── output/
│   ├── similar_trades.csv                # Output from similar_trades()
│   ├── categorize_matching.csv           # Output from categorize_matching()
│   └── configurable_behavior.csv         # Output from configurable_behavior()
├── config.json                           # Config file for mode selection
├── trade_analysis.py                     # Main Python script
└── README.md                             # This documentation
```

---

##  How to Run

1. Place your CSV trade file in the `data/` directory.
2. Ensure you have a `config.json` file like this:

```json
{
  "mode": "B"
}
```

- Mode `"A"`: Ignores same-account checks
- Mode `"B"`: Flags if a user violates trade behavior rules

3. Run the main script:

```bash
python trade_analysis.py
```

---

##  Output

- `output/similar_trades.csv`: All trade pairs matching symbol and timing (excluding same account).
- `output/categorize_matching.csv`: Trade pairs categorized by type.
- `output/configurable_behavior.csv`: Trade pairs with behavior classification and violation info.

---

##  Assumptions Made

- Trades with `lot_size < 0.01` or duration ≤ 1 second are excluded.
- Trades are considered **similar** if they occur on the same symbol and open/close within ±5 minutes.
- "Partial Copy" is defined as a size difference ratio < 30%.
- Timestamps are assumed to be in the same timezone and properly parsed.

---

## Design Decisions

### Why pandas and in-memory operations?
- pandas provides fast, flexible data filtering and grouping.
- Ideal for working with structured tabular data and rapid prototyping.

### Added optimal_sol.py file which gives faster response than last one. 

### Thank you

