import mysql.connector
import socket
import sys
from datetime import date, datetime, timedelta
from hl7apy.parser import parse_message  # Message Parser
from random import randint

def start_server():
    sock = socket.socket()
    server_address = ('localhost', 9009)
    sock.bind(server_address)
    sock.listen(1)
    return sock

def mysql_connection_start():
    conn_b = mysql.connector.connect(   user = 'rooterino2',
                                        password = 'rooterino2',
                                        host = '127.0.0.1',
                                        database = 'fe02_hl7_db_b'
                                    )
    return conn_b    

def end_mysql_connection(conn_b):

    conn_b.close()

def db_insert(m, conn_b):
    
    cursor_b = conn_b.cursor()

    ### Parse message ###

    row_id = m.msh.msh_10.value

    patient_id = m.ORM_O01_PATIENT.pid.pid_2.value

    process_id = m.ORM_O01_PATIENT.pid.pid_3.value

    request_id = m.ORM_O01_ORDER.ORC.orc_2.value

    episode_id = m.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.ORM_O01_OBSERVATION.obx.obx_5.value
    
    sql = "insert into worklist (id, patient_id, process, request_id, episode_id) values (%s, %s, %s, %s, %s)"

    val = (row_id, patient_id, process_id, request_id, episode_id)

    cursor_b.execute(sql, val)

    conn_b.commit()     

    row_id = cursor_b.rowcount
    
    cursor_b.close()
    
    return row_id

def main():
    
    sock = start_server()

    conn_b = mysql_connection_start()

    while_var = 1
    
    """
    # create the connection - batch send
    connection, client_address = sock.accept()
    """
    
    while (while_var):
        
        # create the connection - sequential
        connection, client_address = sock.accept()
        
        data = connection.recv(1024)

        data = data.decode('utf-8')
        
        # if it's not an HL7 message breaks the cycle
        if (data[0] != 'M'):
            while_var = 0
            message = "ACK"
            connection.send(message.encode('utf-8'))

        # if it is, handles the message and inserts it into the db
        else:
            message = parse_message(data)
            row_id = db_insert(message, conn_b)
            if (row_id):
                message = "ACK"
            else:
                message = "Insert failed"
            connection.send(message.encode('utf-8'))
        
        
        # Clean up the connection
        connection.close()
        
    """
    # Clean up the connection
    connection.close()
    """
    end_mysql_connection(conn_b)

    sock.shutdown(1)
    sock.close()
      

if __name__ == '__main__':
    main()