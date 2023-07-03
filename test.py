import socket
import struct
import threading
from confluent_kafka import Producer

kafka_bootstrap_servers='localhost:9092'
kafka_topic='option-data'

producer=Producer({'bootstrap.servers':kafka_bootstrap_servers})
def find_first_number_index(string):
    for i, char in enumerate(string):
        if char.isdigit():
            return i
    return -1  # Return -1 if no number is found in the string

def get_slice_until_index(string, index):
    if index >= 0:
        return string[:index]
    else:
        return string
def process_packet(packet):
    try:
        packet_size, symbol, sequence, timestamp, ltp, ltq, volume, bid_price, bid_qty, ask_price, ask_qty, open_interest, prev_close, prev_open_interest = struct.unpack(
            '<I30sQQQQQQQQQQQQ', packet)
    except struct.error as e:
        print("Error unpacking fields:",e)
        return
    symbol = symbol.decode('utf-8').rstrip('\x00')

    index=find_first_number_index(symbol)
    underlying=get_slice_until_index(symbol,index)
    if index!=-1:
        expiry_date=symbol[index:index+7]
    else:
        expiry_date='-'

    if index!=-1:
        type=symbol[-2:]
    else:
        type='-'

    if type!='XX':
        strike=int(symbol[index+7:-2])
    else:
        strike=0

    if type=='CE': type='Call'
    if type=='PE':type='Put'



    packet_data = {
        'symbol': symbol,
        'underlying':underlying,
        'expiry_date':expiry_date,
        'strike-price':strike,
        'type':type,
        'sequence': sequence,
        'timestamp': timestamp,
        'ltp': ltp,
        'ltq': ltq,
        'volume': volume,
        'bid_price': bid_price,
        'bid_qty': bid_qty,
        'ask_price': ask_price,
        'ask_qty': ask_qty,
        'open_interest': open_interest,
        'prev_close': prev_close,
        'prev_open_interest': prev_open_interest
    }
    print(packet_data)
    producer.produce(kafka_topic,key=symbol,value=str(packet_data))
    producer.flush()

def receive_packets(client_socket):
    while True:
        try:
            data = client_socket.recv(130)
        except socket.timeout:
            print("Timeout occured")
            break
        except OSError as e:
            print("Socket error:",e)
            break

        packet_size=len(data)
        if packet_size!=130:
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
    receive_thread=threading.Thread(target=receive_packets,args=(client_socket,))
    receive_thread.start()

    receive_thread.join()

finally:
    client_socket.close()
