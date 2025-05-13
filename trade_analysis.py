import json
from datetime import timedelta
from itertools import combinations
import pandas as pd

def similar_trades(df):
    # Filter by bonus conditions. we used these two condition together to sort the data.
    filtered_df = df[
        (df['lot_size'] >= 0.01) &
        ((df['closed_at'] - df['opened_at']) > timedelta(seconds=1))
    ].copy()

    # Reset index for efficient cross-comparison.
    filtered_df.reset_index(drop=True, inplace=True)

    # Prepare to find similar trades
    matches = []

    # Compare each pair of trades
    for i in range(len(filtered_df)):
        for j in range(i + 1, len(filtered_df)):
            trade1 = filtered_df.loc[i]
            trade2 = filtered_df.loc[j]

            # Different accounts
            if trade1['trading_account_login'] == trade2['trading_account_login']:
                continue

            # Same symbol
            if trade1['symbol'] != trade2['symbol']:
                continue

            # Open and close times within ±5 minutes
            if (abs((trade1['opened_at'] - trade2['opened_at']).total_seconds()) <= 300 and
                abs((trade1['closed_at'] - trade2['closed_at']).total_seconds()) <= 300):
                matches.append({
                    'symbol': trade1['symbol'],
                    'trade1_id': trade1['identifier'],
                    'account1': trade1['trading_account_login'],
                    'trade2_id': trade2['identifier'],
                    'account2': trade2['trading_account_login'],
                    'open_diff_sec': abs((trade1['opened_at'] - trade2['opened_at']).total_seconds()),
                    'close_diff_sec': abs((trade1['closed_at'] - trade2['closed_at']).total_seconds()),
                })
# these data are in list so we need to convert in DF after it will generate the CSV
    matches = pd.DataFrame(matches)
    matches.to_csv('output/similar_trades.csv', index=False)


def categorize_matching(df):
    # Function to categorize trade pairs
    def categorize_trades(trade1, trade2):
        direction_same = trade1['action'] == trade2['action']
        lot1, lot2 = trade1['lot_size'], trade2['lot_size']
        size_diff_ratio = abs(lot1 - lot2) / max(lot1, lot2) # 0.2/0.3

        if direction_same and size_diff_ratio == 0:
            return "Copy Trade"
        elif not direction_same and size_diff_ratio == 0:
            return "Reverse Trade"
        elif size_diff_ratio < 0.3:
            return "Partial Copy"
        else:
            return "No Match"

    # Filter only relevant columns
    trades = df[['identifier', 'symbol', 'action', 'lot_size', 'opened_at']].copy()
    # trades = df.copy()

    # Sort by symbol and time for pairing
    trades.sort_values(by=['symbol', 'opened_at'], inplace=True)

    # Find pairs of trades on the same symbol
    results = []
    for symbol in trades['symbol'].unique():
        symbol_trades = trades[trades['symbol'] == symbol]
        for trade1, trade2 in combinations(symbol_trades.to_dict('records'), 2):
            category = categorize_trades(trade1, trade2)
            if category != "No Match":
                results.append({
                    'Trade 1 ID': trade1['identifier'],
                    'Trade 2 ID': trade2['identifier'],
                    'Symbol': symbol,
                    'Category': category
                })

    # Create a dataframe of the results
    matched_trades_df = pd.DataFrame(results)
    matched_trades_df.to_csv('output/categorize_matching.csv', index=False)


def configurable_behavior(df):
    # Re-define function to include account check
    def categorize_trades_configurable(trade1, trade2, mode):
        direction_same = trade1['action'] == trade2['action']
        direction_opposite = not direction_same
        lot1, lot2 = trade1['lot_size'], trade2['lot_size']
        size_diff_ratio = abs(lot1 - lot2) / max(lot1, lot2)
        same_user = trade1['trading_account_login'] == trade2['trading_account_login']

        if direction_same and size_diff_ratio == 0:
            category = "Copy Trade"
        elif direction_opposite and size_diff_ratio == 0:
            category = "Reverse Trade"
        elif size_diff_ratio < 0.3:
            category = "Partial Copy"
        else:
            return None, None

        if mode == "A":
            return category, False
        elif mode == "B":
            if same_user:
                return category, True  # Violation
            else:
                return category, False
        return None, None

    with open("config.json") as f:
        config = json.load(f)

    # Load relevant columns including trading account
    trades = df[['identifier', 'symbol', 'action', 'lot_size', 'opened_at', 'trading_account_login']].copy()
    trades.sort_values(by=['symbol', 'opened_at'], inplace=True)

    # Perform comparison with configuration
    results_configurable = []
    for symbol in trades['symbol'].unique():
        symbol_trades = trades[trades['symbol'] == symbol]
        for trade1, trade2 in combinations(symbol_trades.to_dict('records'), 2):
            category, violation = categorize_trades_configurable(trade1, trade2, config["mode"])

            if category:
                results_configurable.append({
                    'Trade 1 ID': trade1['identifier'],
                    'Trade 2 ID': trade2['identifier'],
                    'Symbol': symbol,
                    'Category': category,
                    'Mode': config["mode"],
                    'Same User': trade1['trading_account_login'] == trade2['trading_account_login'],
                    'Violation': violation,
                    'User1': trade1['trading_account_login'],
                    'User2': trade2['trading_account_login'],
                })

    # Create DataFrame of the results
    matched_trades_config_df = pd.DataFrame(results_configurable)
    matched_trades_config_df.to_csv('output/configurable_behavior.csv', index=False)


# Load the CSV file
file_path = 'data/test_task_trades_short.csv'
dff = pd.read_csv(file_path)

# Convert to datetime
dff['opened_at'] = pd.to_datetime(dff['opened_at'])
dff['closed_at'] = pd.to_datetime(dff['closed_at'])

similar_trades(dff)
categorize_matching(dff)
configurable_behavior(dff)