import pandas as pd
import sys

def main(input_file):
    sp500_stock_data = pd.read_csv(input_file)

    # Only keeps relavent columns
    sp500_stock_data = sp500_stock_data[['date', 'change_percent', 'volume']]

    # Removes rows with invalid data
    sp500_stock_data = sp500_stock_data.dropna()

    sp500_stock_data.to_csv("etl_data/cleaned_sp500_stock_data.csv", mode="w", index=False)

if __name__ == "__main__":
    input_file = sys.argv[1]
    main(input_file)