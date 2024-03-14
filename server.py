import avro.datafile
import avro.io
import io
import socket

def handle_client (connection, address):
    data = connection.recv(1024)
    message_buf = io.BytesIO(data)
    reader = avro.datafile.DataFileReader(message_buf, 
        avro.io.DatumReader())
    for thing in reader:
        print(thing)
    reader.close()

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 12345))
    sock.listen(10)

    while True:
        conn, addr = sock.accept()
        handle_client(conn, addr)
        conn.close()

if __name__ == "__main__":
    main()