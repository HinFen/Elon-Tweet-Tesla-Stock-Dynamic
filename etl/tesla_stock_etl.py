import pandas as pd
import sys

def main(input_file):
    tesla_stock_data = pd.read_csv(input_file)

    # Only keeps relavent columns
    tesla_stock_data = tesla_stock_data[['date', 'change_percent', 'volume']]

    # Removes rows with invalid data
    tesla_stock_data = tesla_stock_data.dropna()

    tesla_stock_data.to_csv("etl_data/cleaned_tesla_stock_data.csv", mode='w', index=False)

if __name__ == "__main__":
    input_file = sys.argv[1]
    main(input_file)