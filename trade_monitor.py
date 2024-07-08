import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime, time as dt_time, timedelta
import time
import json
import requests
from dhanhq import dhanhq
import schedule
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
RDS_HOST = os.getenv('RDS_HOST')
RDS_PORT = int(os.getenv('RDS_PORT', 3306))
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_DATABASE = os.getenv('RDS_DATABASE')

# Dhan API client credentials
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')

# Schedule settings
START_TIME = dt_time.fromisoformat(os.getenv('START_TIME', '05:14:00'))
END_TIME = dt_time.fromisoformat(os.getenv('END_TIME', '15:30:00'))
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '100'))  # in seconds

dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

def get_db_connection():
    return pymysql.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DATABASE,
        cursorclass=DictCursor
    )

def get_price(security_id):
    for endpoint in ['prices', 'prices1']:
        url = f'http://ec2-54-242-226-103.compute-1.amazonaws.com:8000/{endpoint}'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and str(security_id) in data:
                    price_info = data[str(security_id)]
                    if isinstance(price_info, dict):
                        return price_info.get('latest_price')
                    elif isinstance(price_info, list) and price_info:
                        return price_info[0].get('price')
            else:
                logging.error(f"Failed to fetch {endpoint}. Status code: {response.status_code}")
        except requests.RequestException as e:
            logging.error(f"Error fetching {endpoint}: {e}")
    logging.error(f"No price data available for security ID {security_id}")
    return None

def calculate_realized_profit(entry_price, exit_price, quantity, trade_type):
    try:
        entry_price = float(entry_price)
        exit_price = float(exit_price)
        quantity = float(quantity)
        
        if trade_type.lower() == 'long':
            profit = (exit_price - entry_price) * quantity
        elif trade_type.lower() == 'short':
            profit = (entry_price - exit_price) * quantity
        else:
            logging.error(f"Invalid trade type: {trade_type}")
            return 0
        
        logging.info(f"Calculated realized profit: {profit}")
        return profit
    except ValueError as e:
        logging.error(f"Error converting values in calculate_realized_profit: {e}")
        return 0

def update_order_status(trade_id, status="closed", entry_price=None, exit_price=None, trade_type=None, quantity=None):
    try:
        exit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        realized_profit = None
        if all([entry_price, exit_price, trade_type, quantity]):
            realized_profit = calculate_realized_profit(entry_price, exit_price, quantity, trade_type)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        UPDATE trades 
        SET order_status = %s, exit_price = %s, exit_time = %s, realized_profit = %s 
        WHERE id = %s
        """
        cursor.execute(query, (status, exit_price, exit_time, realized_profit, trade_id))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Trade ID {trade_id} updated to status '{status}' with exit time '{exit_time}' and realized profit '{realized_profit}'")
    except pymysql.Error as e:
        logging.error(f"An error occurred while updating trade ID {trade_id} to status '{status}': {e}")

def check_and_trigger_orders():
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        today = datetime.now().strftime('%Y-%m-%d')
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        logging.info(f"Fetching trades from {three_days_ago} to {today} for comparison")

        query = """
        SELECT * FROM trades 
        WHERE (order_status = 'success' OR order_status = 'open') 
        AND DATE(timestamp) BETWEEN %s AND %s 
        ORDER BY id DESC
        """
        cursor.execute(query, (three_days_ago, today))
        trades = cursor.fetchall()

        if not trades:
            logging.info(f"No trades found with status 'success' or 'open' for the last three days ({three_days_ago} to {today})")
        else:
            logging.info(f"Found {len(trades)} trades with status 'success' or 'open'")

        for trade in trades:
            trade_id = trade['id']
            symbol = trade['symbol']
            entry_price = trade['entry_price']
            security_id = trade['security_id']
            quantity = trade['quantity']
            stop_loss = trade['stop_loss']
            target = trade['target']
            trade_type = trade['trade_type']
            product_type = trade['product_type']
            
            product_type = str(product_type).upper()

            if product_type not in ['MARGIN', 'INTRADAY']:
                logging.error(f"Invalid product_type {product_type} for trade ID {trade_id}")
                continue

            logging.info(f"Processing trade ID {trade_id}: Symbol = {symbol}, Security ID = {security_id}, Quantity = {quantity}, Stop Loss = {stop_loss}, Target = {target}")

            current_price = get_price(security_id)

            if current_price is None:
                logging.error(f"No current price found for security ID {security_id}")
                continue

            if trade_type.lower() == 'long':
                if current_price <= stop_loss:
                    logging.info(f"Stop loss hit for long {symbol} at price {current_price}")
                    response = dhan.place_order(
                        security_id=str(security_id), 
                        exchange_segment=dhan.NSE_FNO,
                        transaction_type=dhan.SELL,
                        quantity=quantity,
                        order_type=dhan.MARKET,
                        product_type=product_type,
                        price=0
                    )
                    logging.info(f"Stop loss order executed for long {symbol}: {response}")
                    update_order_status(trade_id, status="closed", entry_price=entry_price, exit_price=current_price, trade_type=trade_type, quantity=quantity)
                elif current_price >= target:
                    logging.info(f"Target hit for long {symbol} at price {current_price}")
                    response = dhan.place_order(
                        security_id=str(security_id), 
                        exchange_segment=dhan.NSE_FNO,
                        transaction_type=dhan.SELL,
                        quantity=quantity,
                        order_type=dhan.MARKET,
                        product_type=product_type,
                        price=0
                    )
                    logging.info(f"Target order executed for long {symbol}: {response}")
                    update_order_status(trade_id, status="closed", entry_price=entry_price, exit_price=current_price, trade_type=trade_type, quantity=quantity)
            elif trade_type.lower() == 'short':
                if current_price >= stop_loss:
                    logging.info(f"Stop loss hit for short {symbol} at price {current_price}")
                    response = dhan.place_order(
                        security_id=str(security_id), 
                        exchange_segment=dhan.NSE_FNO,
                        transaction_type=dhan.BUY,
                        quantity=quantity,
                        order_type=dhan.MARKET,
                        product_type=product_type,
                        price=0
                    )
                    logging.info(f"Stop loss order executed for short {symbol}: {response}")
                    update_order_status(trade_id, status="closed", entry_price=entry_price, exit_price=current_price, trade_type=trade_type, quantity=quantity)
                elif current_price <= target:
                    logging.info(f"Target hit for short {symbol} at price {current_price}")
                    response = dhan.place_order(
                        security_id=str(security_id), 
                        exchange_segment=dhan.NSE_FNO,
                        transaction_type=dhan.BUY,
                        quantity=quantity,
                        order_type=dhan.MARKET,
                        product_type=product_type,
                        price=0
                    )
                    logging.info(f"Target order executed for short {symbol}: {response}")
                    update_order_status(trade_id, status="closed", entry_price=entry_price, exit_price=current_price, trade_type=trade_type, quantity=quantity)

    except pymysql.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def is_weekday():
    return datetime.now().weekday() < 5

def is_work_hours():
    now = datetime.now().time()
    return START_TIME <= now <= END_TIME

def job():
    if is_weekday() and is_work_hours():
        try:
            check_and_trigger_orders()
        except Exception as e:
            logging.error(f"An error occurred while checking orders: {e}")
    else:
        logging.info("Outside of work hours. Sleeping...")

def main():
    schedule.every(CHECK_INTERVAL).seconds.do(job)
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}")
            time.sleep(CHECK_INTERVAL)  # If an error occurs, wait before retrying

if __name__ == "__main__":
    main()