import avro.datafile
import avro.io
import io
import socket
import struct

class Disconnect(Exception):
    pass

def read_block(connection, message_size):
    bytes_read = 0
    block = b''
    while bytes_read < message_size:
        data = connection.recv(message_size - bytes_read)
        if len(data) == 0:
            raise Disconnect()
        block += data
        bytes_read += len(data)
        print("Read {} bytes".format(len(data)))
    print("Read {} byte block".format(len(block)))
    return block

def handle_client (connection, address):
    try:
        while True:
            size_block = read_block(connection, 4)
            message_size, = struct.unpack("!L", size_block)
            message_block = read_block(connection, message_size)
            message_buf = io.BytesIO(message_block)
            reader = avro.datafile.DataFileReader(message_buf, 
                avro.io.DatumReader())
            for message in reader:
                print(message)
            reader.close()
    except Disconnect as e:
        print("Client disconnected")

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