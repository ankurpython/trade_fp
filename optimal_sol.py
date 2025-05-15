import os
import json
import pandas as pd
from datetime import timedelta

# Ensure output directory exists
os.makedirs('output', exist_ok=True)

def optimized_similar_trades(df):
    df = df[
        (df['lot_size'] >= 0.01) &
        ((df['closed_at'] - df['opened_at']) > timedelta(seconds=1))
    ].copy()

    df['open_bucket'] = df['opened_at'].dt.floor('5min')
    df['close_bucket'] = df['closed_at'].dt.floor('5min')

    df = df[['identifier', 'symbol', 'opened_at', 'closed_at', 'lot_size', 'trading_account_login', 'open_bucket', 'close_bucket']]

    matches = []

    grouped = df.groupby(['symbol', 'open_bucket', 'close_bucket'])

    for _, group in grouped:
        merged = group.merge(group, on='symbol', suffixes=('_1', '_2'))
        merged = merged[
            (merged['identifier_1'] < merged['identifier_2']) &
            (merged['trading_account_login_1'] != merged['trading_account_login_2']) &
            (abs((merged['opened_at_1'] - merged['opened_at_2']).dt.total_seconds()) <= 300) &
            (abs((merged['closed_at_1'] - merged['closed_at_2']).dt.total_seconds()) <= 300)
        ]

        matches.append(merged[[
            'symbol', 'identifier_1', 'trading_account_login_1',
            'identifier_2', 'trading_account_login_2',
            'opened_at_1', 'opened_at_2', 'closed_at_1', 'closed_at_2'
        ]])

    if matches:
        final_matches = pd.concat(matches, ignore_index=True)
        final_matches['open_diff_sec'] = (final_matches['opened_at_1'] - final_matches['opened_at_2']).abs().dt.total_seconds()
        final_matches['close_diff_sec'] = (final_matches['closed_at_1'] - final_matches['closed_at_2']).abs().dt.total_seconds()

        final_matches.to_csv('output/optimized_similar_trades.csv', index=False)
    else:
        print("No similar trades found.")


def optimized_categorize_matching(df):
    df = df[['identifier', 'symbol', 'action', 'lot_size', 'opened_at']].copy()
    df['open_bucket'] = df['opened_at'].dt.floor('5min')

    results = []

    for symbol, group in df.groupby('symbol'):
        merged = group.merge(group, on='symbol', suffixes=('_1', '_2'))
        merged = merged[
            (merged['identifier_1'] < merged['identifier_2'])
        ]

        merged['size_diff_ratio'] = (merged['lot_size_1'] - merged['lot_size_2']).abs() / merged[['lot_size_1', 'lot_size_2']].max(axis=1)
        merged['direction_same'] = merged['action_1'] == merged['action_2']

        def categorize(row):
            if row['size_diff_ratio'] == 0 and row['direction_same']:
                return 'Copy Trade'
            elif row['size_diff_ratio'] == 0 and not row['direction_same']:
                return 'Reverse Trade'
            elif row['size_diff_ratio'] < 0.3:
                return 'Partial Copy'
            return 'No Match'

        merged['Category'] = merged.apply(categorize, axis=1)
        merged = merged[merged['Category'] != 'No Match']

        results.append(merged[[
            'identifier_1', 'identifier_2', 'symbol', 'Category'
        ]].rename(columns={
            'identifier_1': 'Trade 1 ID',
            'identifier_2': 'Trade 2 ID',
        }))

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df.to_csv('output/optimized_categorize_matching.csv', index=False)
    else:
        print("No categorized matches found.")


def optimized_configurable_behavior(df):
    with open("config.json") as f:
        config = json.load(f)
    mode = config["mode"]

    df = df[['identifier', 'symbol', 'action', 'lot_size', 'opened_at', 'trading_account_login']].copy()
    df['open_bucket'] = df['opened_at'].dt.floor('5min')

    results = []

    for symbol, group in df.groupby('symbol'):
        merged = group.merge(group, on='symbol', suffixes=('_1', '_2'))
        merged = merged[merged['identifier_1'] < merged['identifier_2']]

        merged['size_diff_ratio'] = (merged['lot_size_1'] - merged['lot_size_2']).abs() / merged[['lot_size_1', 'lot_size_2']].max(axis=1)
        merged['direction_same'] = merged['action_1'] == merged['action_2']
        merged['same_user'] = merged['trading_account_login_1'] == merged['trading_account_login_2']

        def categorize(row):
            category = None
            violation = False
            if row['size_diff_ratio'] == 0 and row['direction_same']:
                category = 'Copy Trade'
            elif row['size_diff_ratio'] == 0 and not row['direction_same']:
                category = 'Reverse Trade'
            elif row['size_diff_ratio'] < 0.3:
                category = 'Partial Copy'
            else:
                return None, None

            if mode == "A":
                return category, False
            elif mode == "B":
                if row['same_user']:
                    return category, True
                else:
                    return category, False
            return None, None

        # Safe row-wise application
        categories, violations = [], []
        for _, row in merged.iterrows():
            cat, vio = categorize(row)
            categories.append(cat)
            violations.append(vio)

        merged['Category'] = categories
        merged['Violation'] = violations
        merged = merged[merged['Category'].notnull()]

        results.append(merged[[
            'identifier_1', 'identifier_2', 'symbol', 'Category',
            'trading_account_login_1', 'trading_account_login_2',
            'same_user', 'Violation'
        ]].rename(columns={
            'identifier_1': 'Trade 1 ID',
            'identifier_2': 'Trade 2 ID',
            'trading_account_login_1': 'User1',
            'trading_account_login_2': 'User2',
            'same_user': 'Same User'
        }))

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df['Mode'] = mode
        final_df.to_csv('output/optimized_configurable_behavior.csv', index=False)
    else:
        print("No configurable behavior matches found.")



# ===== MAIN SCRIPT =====

if __name__ == "__main__":
    # Load your data
    file_path = 'data/test_task_trades_short.csv'
    dff = pd.read_csv(file_path)

    # Convert date columns
    dff['opened_at'] = pd.to_datetime(dff['opened_at'])
    dff['closed_at'] = pd.to_datetime(dff['closed_at'])

    # Run all analyses
    print("Running optimized_similar_trades...")
    optimized_similar_trades(dff)

    print("Running optimized_categorize_matching...")
    optimized_categorize_matching(dff)

    print("Running optimized_configurable_behavior...")
    optimized_configurable_behavior(dff)

    print("done. Check the output folder.")
