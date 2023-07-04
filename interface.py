import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

def run_streamlit_app():
    st.title("Options Data Table")
    index = st.selectbox('Choose a Symbol', ['MAINIDX', 'ALLBANKS', 'FINANCIALS'])
    types = st.selectbox('Call or Put', ['Call', 'Put'])
    expiry= st.selectbox('Expiry', ['06JUL23', '13JUL23','31AUG23'])
    # Set up Kafka consumer
    consumer = KafkaConsumer("option-data", bootstrap_servers=["localhost:9092"],enable_auto_commit=True,  # Enable auto-commit
    auto_commit_interval_ms=100,  # Set auto-commit interval to 1 millisecond
    auto_offset_reset="earliest")

    # Initialize an empty list to store stock data
    #stock_data = [["Symbol", "Underlying", "Expiry_date"]]  # List of lists with column headers

    # Create a Streamlit table component
    table_data=[[0,0,0,0,0,0,0,0,0,0,0]]
    data1 = pd.DataFrame(table_data,columns=["OI", "Change in OI", "Volume","IV","LTP","CHNG","BID QTY","BID","ASK", "ASK QTY", "STRIKE"])
    table1 = st.table(data1)
    data2 =pd.DataFrame(table_data,columns=["STRIKE","BID QTY","BID","ASK", "ASK QTY","CHNG","LTP","IV","VOLUME","CHNG IN OI","OI"])
    table2 =st.table(data2)
    #message_value="{'open_interest': 0, 'change_in_oi': -5600, 'volume': 0, 'iv': 72615.0, 'ltp': 72615, 'change_price': 66415, 'bid_qty': 200, 'bid_price': 71860, 'ask_price': 74275, 'ask_qty': 100, 'strike-price': 19350, 'symbol': 'MAINIDX27JUL2319350PE', 'underlying': 'MAINIDX', 'expiry_date': '27JUL23', 'type': 'Put', 'sequence': 1233, 'timestamp': 1688483201960, 'ltq': 0, 'prev_close': 6200, 'prev_open_interest': 5600}"
    
    # Continuously listen for new messages from Kafka
    
    for message in consumer:
        # Append received stock data to the list
        
        message_value=message.value
        message_value = message_value.replace(b"'", b'"')
        message_dict = json.loads(message_value.decode('utf-8'))
        
        # Access the values for open_interest and change_in_oi
        
        try:
            SYMB = message_dict['underlying']
            EXPDTE = message_dict['expiry_date']
            TYPE = message_dict['type']
            OI = message_dict['open_interest']
            CHNGOI = message_dict['change_in_oi']
            VLM = message_dict['volume']
            IV = message_dict['iv']
            LTP = message_dict['ltp']
            CHNG = message_dict['change_price']
            BIDQTY = message_dict['bid_qty']
            BID = message_dict['bid_price']
            ASK = message_dict['ask_price']
            ASKQTY = message_dict['ask_qty']
            STRK = message_dict['strike-price']
        except KeyError:
            # Assign default values of 0 if any key is missing in the message_dict
            SYMB = 0
            EXPDTE = 0
            TYPE = 0
            OI = 0
            CHNGOI = 0
            VLM = 0
            IV = 0
            LTP = 0
            CHNG = 0
            BIDQTY = 0
            BID = 0
            ASK = 0
            ASKQTY = 0
            STRK = 0

        if SYMB==index and TYPE==types:
            #new_row1={"OI":OI, "CHNG IN OI":CHNGOI, "Volume":VLM,"IV":IV,"LTP":LTP,"CHNG":CHNG,"BID QTY":BIDQTY,"BID":BID,"ASK":ASK, "ASK QTY":ASKQTY, "STRIKE":STRK}
            new_row=[OI, CHNGOI,VLM,IV/1000,LTP,CHNG,BIDQTY,BID,ASK, ASKQTY, STRK]
            table_data.append(new_row)

            # Display the updated table
            updated_table = pd.DataFrame(table_data, columns=["OI", "Change in OI", "Volume","IV","LTP","CHNG","BID QTY","BID","ASK", "ASK QTY", "STRIKE"])
            table1.table(updated_table)
            print(new_row)
            
            #new_row_df = pd.DataFrame([new_row1])
            #data=data.append(new_row_df, ignore_index=True)
        #stock_data.append([message["symbol"], message["underlying"], message["expiry_date"]])

        # Update the table with the new stock data
        #table1.data = data
        #message.commit()

    #table(data)
# Run the Streamlit app
if __name__ == "__main__":
    run_streamlit_app()
