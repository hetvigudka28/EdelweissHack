import socket
import struct

#code run on terminal is:  java -Ddebug=true -Dspeed=1.0 -classpath feed-play-1.0.jar hackathon.player.Main dataset.csv 9011

# Define the server address and port
server_address = ('localhost', 9011)

# Create a TCP/IP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Set a timeout value of 10 seconds
client_socket.settimeout(600)

try:
    # Connect to the server
    client_socket.connect(server_address)

    # Send the initial request
    client_socket.sendall(b'\x01')  # Sending a single byte packet indicating readiness

    # Receive the snapshot data from the server
    snapshot_data = client_socket.recv(130)  # Assuming the snapshot data packet size is fixed at 130 bytes
    print("Received snapshot data size:", len(snapshot_data))
    print("Snapshot Data:", snapshot_data)

    # Unpack the snapshot data
    packet_size, symbol, sequence, timestamp, ltp, ltq, volume, bid_price, bid_qty, ask_price, ask_qty, open_interest, prev_close, prev_open_interest = struct.unpack(
        '<I30sQQQQQQQQQQQQ', snapshot_data)

    total = 1
    # Decode the symbol string
    symbol = symbol.decode('utf-8').rstrip('\x00')

    # Print the unpacked fields
    print("Unpacked snapshot data:")
    print("Symbol:", symbol)
    print("Sequence Number:", sequence)
    print("Timestamp:", timestamp)
    print("Last Traded Price (LTP):", ltp)
    print("Last Traded Quantity (LTQ):", ltq)
    print("Volume:", volume)
    print("Bid Price:", bid_price)
    print("Bid Quantity:", bid_qty)
    print("Ask Price:", ask_price)
    print("Ask Quantity:", ask_qty)
    print("Open Interest:", open_interest)
    print("Previous Close Price:", prev_close)
    print("Previous Open Interest:", prev_open_interest)

    # Continue receiving real-time updates from the server
    while True:
        print("TOTAL PACKETS SEEN:", total)

        try:
            data = client_socket.recv(130)
        except socket.timeout:
            # Handle timeout, no data received within the timeout period
            print("Timeout occurred. No data received.")
            break



        # Unpack the real-time update data
        packet_size = struct.unpack('<I', data[:4])[0]

        if packet_size != 124:
            print("Invalid packet size. Skipping packet:", packet_size)
            continue

        # Unpack the remaining fields
        print(packet_size)
        try:
            symbol, sequence, timestamp, ltp, ltq, volume, bid_price, bid_qty, ask_price, ask_qty, open_interest, prev_close, prev_open_interest = struct.unpack(
                '<30sQQQQQQQQQQQQ', data[4:])
        except struct.error as e:
            print("Error unpacking fields:", e)
            continue
        # Decode the symbol string
        symbol = symbol.decode('utf-8').rstrip('\x00')

        # Print the unpacked fields
        print("Packet Size:", packet_size)
        print("Symbol:", symbol)
        print("Sequence Number:", sequence)
        print("Timestamp:", timestamp)
        print("Last Traded Price (LTP):", ltp)
        print("Last Traded Quantity (LTQ):", ltq)
        print("Volume:", volume)
        print("Bid Price:", bid_price)
        print("Bid Quantity:", bid_qty)
        print("Ask Price:", ask_price)
        print("Ask Quantity:", ask_qty)
        print("Open Interest:", open_interest)
        print("Previous Close Price:", prev_close)
        print("Previous Open Interest:", prev_open_interest)

        total += 1

finally:
    # Close the client socket when done or in case of an error
    client_socket.close()
