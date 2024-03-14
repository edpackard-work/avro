import io
import socket
import avro.datafile
import avro.schema
import avro.io
import avro.ipc
import random
import struct
import time

SCHEMA = avro.schema.parse(open("./user.avsc").read())
NAMES = ["Reece","Gordon","Kaeli","Dominique","Shakira","Nolan","Joanne","Kaylie","Javion","Kala","Keanu","Haleigh","Rianna","Elian","Juan","Leroy","Marc", "Pranav","Kobi","Dustin","Johnson","Liam","Malik","Sincere","Madisen","Jaila","Sianne","Scarlett","Beyonce","Mia"]
COLORS = ["red", "green", "blue", "black", "fuscia"]

def send_message(connection, message):
    buf = io.BytesIO()
    writer = avro.datafile.DataFileWriter(buf, 
        avro.io.DatumWriter(), SCHEMA)
    writer.append(message)
    writer.flush()
    data_length = buf.tell()
    print("Message size: ", data_length)
    buf.seek(0)
    data= buf.read()
    bytes_written = connection.send(struct.pack("!L", data_length))
    print("Wrote bytes: ", bytes_written)

    connection.send(data)

def main(): 

    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect(("127.0.0.1", 12345))

    for _ in range(20):
        send_message(connection, {
            "name": random.choice(NAMES),
            "favorite_number": random.randint(0,100),
            "favorite_color": random.choice(COLORS)
        })
        time.sleep(random.randint(1, 3))
    
if __name__ == "__main__":
    main()