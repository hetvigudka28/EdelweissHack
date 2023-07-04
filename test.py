import socket
import struct
import threading
from confluent_kafka import Producer
from datetime import datetime
from scipy.stats import norm
from math import log, sqrt, exp

def calculate_call_price(S, K, r, sigma, T):
    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)

    call_price = S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
    return call_price

def find_first_number_index(string):
    for i, char in enumerate(string):
        if char.isdigit():
            return i

    return -1

def get_slice_until_index(string, index):
    if index >= 0:
        return string[:index]
    else:
        return string

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'option-data'
producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

def process_packet(packet):
    try:
        packet_size, symbol, sequence, timestamp, ltp, ltq, volume, bid_price, bid_qty, ask_price, ask_qty, open_interest, prev_close, prev_open_interest = struct.unpack(
            '<I30sQQQQQQQQQQQQ', packet)
    except struct.error as e:
        print("Error unpacking fields:", e)
        return
    symbol = symbol.decode('utf-8').rstrip('\x00')

    index = find_first_number_index(symbol)
    underlying = get_slice_until_index(symbol, index)
    if index != -1:
        expiry_date = symbol[index:index + 7]
    else:
        expiry_date = '-'

    if index != -1:
        type = symbol[-2:]
    else:
        type = '-'

    if type != 'XX':
        strike = int(symbol[index + 7:-2])
    else:
        strike = 0

    if type == 'CE': type = 'Call'
    if type == 'PE': type = 'Put'
    if type == 'Call' or type == 'Put':
        current_datetime = datetime.now()
        expiry_date_str = expiry_date  # Example expiry date string
        expiry_date_date = datetime.strptime(expiry_date_str, "%d%b%y")
        time_to_maturity = expiry_date_date - current_datetime
        days_to_maturity = time_to_maturity.days
        iv = calculate_call_price(ltp, strike, 5, 0.2, days_to_maturity)

    change_in_oi = open_interest - prev_open_interest
    change_price = (ltp - prev_close) if ltp > 0 else 0
    packet_data = {
        "open_interest": open_interest,
        "change_in_oi": change_in_oi,
        "volume": volume,
        "iv": iv if iv else 0,
        "ltp": ltp,
        "change_price": change_price,
        "bid_qty": bid_qty,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "ask_qty": ask_qty,
        "strike-price": strike,
        "symbol": symbol,
        "underlying": underlying,
        "expiry_date": expiry_date,
        "type": type,
        "sequence": sequence,
        "timestamp": timestamp,
        "ltq": ltq,
        "prev_close": prev_close,
        "prev_open_interest": prev_open_interest,
    }
    print(packet_data)
    producer.produce(kafka_topic, key=symbol, value=str(packet_data))
    producer.flush()


def receive_packets(client_socket):
    while True:
        try:
            data = client_socket.recv(130)
        except socket.timeout:
            print("Timeout occured")
            break
        except OSError as e:
            print("Socket error:", e)
            break

        packet_size = len(data)
        if packet_size != 130:
            print("Invalid packet size. Skipping packet, with size:", packet_size)
            continue

        packet_thread = threading.Thread(target=process_packet, args=(data,))
        packet_thread.start()


server_address = ('localhost', 9011)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.settimeout(600)
try:
    client_socket.connect(server_address)
    client_socket.sendall(b'\x01')

    snapshot_data = client_socket.recv(130)
    print("Received snapshot data size:", len(snapshot_data))
    print("Snapshot Data:", snapshot_data)

    process_packet(snapshot_data)
    receive_thread = threading.Thread(target=receive_packets, args=(client_socket,))
    receive_thread.start()

    receive_thread.join()

finally:
    client_socket.close()
