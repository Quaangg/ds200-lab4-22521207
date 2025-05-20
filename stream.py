import pandas as pd
import socket
import time

CSV_FILE_PATH = 'Laptop_price.csv'
HOST = 'localhost'
PORT = 9999
SEND_DELAY_SECONDS = 0.2

try:
    print(f"Reading CSV file: {CSV_FILE_PATH}")
    df = pd.read_csv(CSV_FILE_PATH)
    print(f"Successfully read {len(df)} rows from CSV file.")
except FileNotFoundError:
    print(f"Error: File not found at {CSV_FILE_PATH}.")
    exit()
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

try:
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(f"Data Streamer listening on {HOST}:{PORT}")
    print("Waiting for Spark Streaming to connect...")
    conn, addr = server_socket.accept()
    print(f"Connected by Spark: {addr}")

    with conn:
        for index, row_series in df.iterrows():
            csv_message = ','.join(row_series.astype(str).tolist())

            try:
                conn.sendall((csv_message + '\n').encode('utf-8'))
                print(f"Sent row {index+1}/{len(df)}")
            except (BrokenPipeError, ConnectionResetError):
                print("Client (Spark) disconnected.")
                break
            except Exception as e:
                print(f"Error sending data: {e}")
                break

            time.sleep(SEND_DELAY_SECONDS)
        print("Finished sending all data from CSV.")

except socket.error as e:
    print(f"Socket error: {e}")
except Exception as e:
    print(f"An error occurred on the server: {e}")
finally:
    print("Closing streamer server connection.")
    if 'conn' in locals() and conn:
        try:
            conn.close()
        except: pass
    try:
        server_socket.close()
    except: pass
