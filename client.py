import io
import socket
import avro.datafile
import avro.schema
import avro.io
import avro.ipc

SCHEMA = avro.schema.parse(open("./user.avsc").read())

def send_message(connection, message):
    buf = io.BytesIO()
    writer = avro.datafile.DataFileWriter(buf, 
        avro.io.DatumWriter(), SCHEMA)
    writer.append(message)
    writer.flush()
    buf.seek(0)
    data= buf.read()
    connection.send(data)

def main(): 
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect(("127.0.0.1", 12345))
    send_message(connection, {"name": "Ben", "favorite_number": 7, "favorite_color": "red"})

if __name__ == "__main__":
    main()