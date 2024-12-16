import yfinance as yf

try:
    tesla = yf.Ticker('TSLA')
    recommendations = tesla.recommendations
    if recommendations is not None:
        print(recommendations['To Grade'].value_counts().keys()[0])
    else:
        print("No recommendations data available.")
except Exception as e:
    print(f"An error occurred: {e}")