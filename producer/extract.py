import requests
import time
from config import logger, headers, url

def connect_to_api():
    stocks = ['TSLA', 'AAPL', 'GOOGL', 'AMZN', 'MSFT']
    json_responses = []
    
    for stock in stocks:
        querystring = {"function":"TIME_SERIES_DAILY","symbol":stock,"outputsize":"compact","datatype":"json"}

        try:
            response = requests.get(url, headers=headers, params=querystring)
            time.sleep(1.5)
            response.raise_for_status()
            data = response.json()
            
            # Use 'Time Series (Daily)' key to verify data exists
            if "Time Series (Daily)" in data:
                logger.info(f"Stock {stock} successfully loaded")
                json_responses.append(data)
            else:
                logger.warning(f"No daily data found for {stock}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to the API for {stock}: {e}")
    
    return json_responses # Return the full list of all stocks data

def extract_json(responses): # Added parameter 'responses'
    records = [] # Fixed typo from 'recorsd'

    for data in responses:
        # Check if the expected keys exist to avoid KeyErrors
        if 'Meta Data' not in data: continue
        
        symbol = data['Meta Data']['2. Symbol']
    
        for date_str, metrics in data['Time Series (Daily)'].items():
            record = {
                'symbol': symbol,
                'date': date_str,
                'open': metrics['1. open'],
                'high': metrics['2. high'],
                'low': metrics['3. low'],
                'close': metrics['4. close'],
                'volume': metrics['5. volume']
            }
            records.append(record)
            
    return records
