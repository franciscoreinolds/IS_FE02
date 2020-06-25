import mysql.connector
import socket
import sys
from datetime import datetime, date, timedelta
from hl7apy import core
from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.parser import parse_message  # Message Parser
from random import randint

class Record_Data:
    def __init__ (self):
        self.patient_id = None
        self.process_id = None
        self.request_id = None
        self.episode_id = None
        self.row_id = None

conn_a = mysql.connector.connect(   user = 'rooterino',
                                    password = 'rooterino',
                                    host = '127.0.0.1',
                                    database = 'fe02_hl7_db_a'
                                )

def generate_insert_data():
    # open cursor
    cursor_a = conn_a.cursor()

    # handle sql + values and finally committing
    sql = "insert into worklist (patient_id, process, request_id, episode_id) values (%s, %s, %s, %s)"

    obj = Record_Data()

    obj.patient_id = randint(1, 2500)
    
    obj.process_id = randint(1, 2500)
    
    obj.request_id = randint(1, 2500)
    
    obj.episode_id = randint(1, 2500)

    val = (obj.patient_id, obj.process_id, obj.request_id, obj.episode_id)

    cursor_a.execute(sql, val)

    conn_a.commit()
    
    # get the row of the inserted id
    obj.row_id = cursor_a.lastrowid

    cursor_a.close()

    return obj

def create_message(obj):
   # Message Creation
    hl7 = core.Message("ORM_O01", validation_level=VALIDATION_LEVEL.STRICT)

    # Message Header
    hl7.msh.msh_3 = "PedidosClient"
    hl7.msh.msh_4 = "PedidosClient"
    hl7.msh.msh_5 = "ExamesServer"
    hl7.msh.msh_6 = "ExamesServer"
    hl7.msh.msh_9 = "ORM^O01^ORM_O01"
    hl7.msh.msh_10 = str(obj.row_id)
    hl7.msh.msh_11 = "P"

    # PID
    hl7.add_group("ORM_O01_PATIENT")
    hl7.ORM_O01_PATIENT.pid.pid_2 = str(obj.patient_id)  # id
    hl7.ORM_O01_PATIENT.pid.pid_3 = str(obj.process_id)  # processo
    hl7.ORM_O01_PATIENT.pid.pid_5 = str("Benchmark")
    hl7.ORM_O01_PATIENT.pid.pid_11 = str("Computer")
    hl7.ORM_O01_PATIENT.pid.pid_13 = str("123456789")

    # PV1
    hl7.ORM_O01_PATIENT.add_group("ORM_O01_PATIENT_VISIT")
    hl7.ORM_O01_PATIENT.ORM_O01_PATIENT_VISIT.add_segment("PV1")
    hl7.ORM_O01_PATIENT.ORM_O01_PATIENT_VISIT.PV1.pv1_1 = "1"
    hl7.ORM_O01_PATIENT.ORM_O01_PATIENT_VISIT.PV1.pv1_2 = "1"

    # ORC
    hl7.add_group("ORM_O01_ORDER")
    hl7.ORM_O01_ORDER.ORC.orc_1 = "NW"
    hl7.ORM_O01_ORDER.ORC.orc_2 = str(obj.request_id)  # idpedido
    hl7.ORM_O01_ORDER.ORC.orc_10 = "2020-04-03"

    # OBR
    hl7.ORM_O01_ORDER.add_group("ORM_O01_ORDER_DETAIL")
    hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.add_group("ORM_O01_OBSERVATION")
    hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.ORM_O01_OBSERVATION.obx.obx_5 = str(obj.episode_id)  # idepisodio
    #print(hl7.value.replace('\r', '\n'))
    return hl7.value

"""
def send_hl7(row_id, patient_id, process_id, request_id, episode_id):
    
    # Message Creation
    hl7 = core.Message("ORM_O01", validation_level=VALIDATION_LEVEL.STRICT)

    # Message Header
    hl7.msh.msh_3 = "PedidosClient"
    hl7.msh.msh_4 = "PedidosClient"
    hl7.msh.msh_5 = "ExamesServer"
    hl7.msh.msh_6 = "ExamesServer"
    hl7.msh.msh_9 = "ORM^O01^ORM_O01"
    hl7.msh.msh_10 = str(row_id)
    hl7.msh.msh_11 = "P"

    # PID
    hl7.add_group("ORM_O01_PATIENT")
    hl7.ORM_O01_PATIENT.pid.pid_2 = str(patient_id)  # id
    hl7.ORM_O01_PATIENT.pid.pid_3 = str(process_id)  # processo
    hl7.ORM_O01_PATIENT.pid.pid_5 = str("Benchmark")
    hl7.ORM_O01_PATIENT.pid.pid_11 = str("Computer")
    hl7.ORM_O01_PATIENT.pid.pid_13 = str("123456789")

    # PV1
    hl7.ORM_O01_PATIENT.add_group("ORM_O01_PATIENT_VISIT")
    hl7.ORM_O01_PATIENT.ORM_O01_PATIENT_VISIT.add_segment("PV1")
    hl7.ORM_O01_PATIENT.ORM_O01_PATIENT_VISIT.PV1.pv1_1 = "1"
    hl7.ORM_O01_PATIENT.ORM_O01_PATIENT_VISIT.PV1.pv1_2 = "1"

    # ORC
    hl7.add_group("ORM_O01_ORDER")
    hl7.ORM_O01_ORDER.ORC.orc_1 = "NW"
    hl7.ORM_O01_ORDER.ORC.orc_2 = str(request_id)  # idpedido
    hl7.ORM_O01_ORDER.ORC.orc_10 = "2020-04-03"

    # OBR
    hl7.ORM_O01_ORDER.add_group("ORM_O01_ORDER_DETAIL")
    #hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.add_segment("ORM_O01_ORDER_CHOICE")
    #hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.add_segment("ORM_O01_ORDER_DETAIL_SEGMENT")
    #hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL_GROUP.add_segment("OBR")
    #hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.ORM_O01_ORDER_CHOICE.OBR.obr_13 = "Observações"
    #hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.ORM_O01_ORDER_CHOICE.OBR.obr_12 = "Relatorio"
    #hl7.ORM_O01_ORDER.ORM_O01_ORDER_DETAIL.ORM_O01_ORDER_CHOICE.OBR.obr_4 = str(episode_id)  # idepisodio

    #print(hl7.value.replace('\r', '\n'))
    return hl7.value
"""

def open_tcp():
    sock = socket.socket()
    server_address = ('localhost', 9009)
    sock.connect(server_address)
    return sock


def close_tcp(sock):
    sock.shutdown(1)
    sock.close()

def send_message(sock, message):

    # Send data
    sock.send(message.encode('utf-8'))
    # Receive data
    received_data = sock.recv(1024)
    data = received_data.decode('utf-8')
    if (data[0] == 'I'):
        #it's insert failed so we need to remove the inserted record
        message = parse_message(data)
        row_id = message.msh.msh_10.value
        delete_record(row_id)            

def delete_record(row_id):
    # open cursor
    cursor_a = conn_a.cursor()
    
    # handle sql + values
    sql = "delete from worklist where id = %s"
    val = (row_id)
    
    # execute sql and commit it
    cursor_a.execute(sql, val)
    conn_a.commit()
    
    # close cursor
    cursor_a.close()


def end_mysql_connection():

    # closes mysql connection
    
    conn_a.close()

def sequential_execute(run_time):
    i = 0
    
    beg_time = None

    end_time = None

    total_time = 0

    total_socket_open_time = 0

    total_socket_close_time = 0

    total_msg_send_time = 0

    total_data_generation_time = 0

    total_msg_creation_time = 0

    today = datetime.now() + timedelta(seconds = run_time)

    while (datetime.now() < today):
        i += 1

        # Generate and Insert data
        beg_time = datetime.now()
        obj = generate_insert_data()
        end_time = datetime.now()
        total_data_generation_time += int(round((end_time.timestamp() - beg_time.timestamp()) * 1000))
        
        # Create message
        beg_time = datetime.now()
        message = create_message(obj)
        end_time = datetime.now()
        total_msg_creation_time += int(round((end_time.timestamp() - beg_time.timestamp()) * 1000))
        
        # If message has been created successfully
        if (message):
            
            # Open socket
            beg_time = datetime.now()
            sock = open_tcp()
            end_time = datetime.now()
            total_socket_open_time += int(((end_time.timestamp() - beg_time.timestamp()) * 1000))
            
            # Send message to System B for processing
            beg_time = datetime.now()
            send_message(sock, message)
            end_time = datetime.now()
            total_msg_send_time += int(round((end_time.timestamp() - beg_time.timestamp()) * 1000))
            
            # Close socket
            beg_time = datetime.now()
            close_tcp(sock)
            end_time = datetime.now()
            total_socket_close_time += int(((end_time.timestamp() - beg_time.timestamp()) * 1000))
            
    total_time = total_socket_open_time + total_socket_close_time + total_msg_send_time + total_msg_creation_time + total_data_generation_time

    print(f'[SEQUENTIAL] Total execution time -> {run_time} (s)')

    print(f'Final HL7 messages tally is : {i}')

    print(f'Total time spent opening sockets -> {total_socket_open_time} (ms)')

    print(f'Total time spent closing sockets -> {total_socket_close_time} (ms)')

    print(f'Total time spent generating and inserting Data ( DB-A ) -> {total_data_generation_time} (ms)')

    print(f'Total time spent creating messages -> {total_msg_creation_time} (ms)')

    print(f'Total time spent sending and waiting for message ACKS -> {total_msg_send_time} (ms)')

    print(f'Total run time -> {total_time} (ms)')

def batch_send(sock, batch_size):

    i = 0

    message_archive = []

    beg_time = None

    end_time = None

    total_time = 0

    total_socket_open_time = 0

    total_socket_close_time = 0

    total_msg_send_time = 0

    total_data_generation_time = 0

    total_msg_creation_time = 0

    while (i < batch_size):
        # Generate and Insert data
        beg_time = datetime.now()
        obj = generate_insert_data()
        end_time = datetime.now()
        total_data_generation_time += int(round((end_time.timestamp() - beg_time.timestamp()) * 1000))
        
        # Create message
        beg_time = datetime.now()
        created_message = create_message(obj)
        end_time = datetime.now()
        total_msg_creation_time += int(round((end_time.timestamp() - beg_time.timestamp()) * 1000))

        if (created_message):
            message_archive.append(created_message)
            i+=1

    beg_time = datetime.now()

    for msg in message_archive:
        #sock = open_tcp()
        #send_message(sock, msg)
        #close_tcp(sock)
        """
        # socket opening
        beg_time = datetime.now()
        sock = open_tcp()
        end_time = datetime.now()
        total_socket_open_time += int(((end_time.timestamp() - beg_time.timestamp()) * 1000))
        """
        # message sending
        beg_time = datetime.now()
        send_message(sock, msg)
        end_time = datetime.now()
        total_msg_send_time += int(round((end_time.timestamp() - beg_time.timestamp()) * 1000))
        """
        # socket closing
        beg_time = datetime.now()
        close_tcp(sock)
        end_time = datetime.now()
        total_socket_close_time += int(((end_time.timestamp() - beg_time.timestamp()) * 1000))
        """
    #close_tcp(sock)

    total_time = total_socket_open_time + total_socket_close_time + total_msg_send_time + total_msg_creation_time + total_data_generation_time

    print(f'Final HL7 messages tally is : {i}')

    print(f'Total time spent opening sockets -> {total_socket_open_time} (ms)')

    print(f'Total time spent closing sockets -> {total_socket_close_time} (ms)')

    print(f'Total time spent creating messages -> {total_msg_creation_time} (ms)')

    print(f'Total time spent generating and inserting Data ( DB-A ) -> {total_data_generation_time} (ms)')

    print(f'Total time spent sending and waiting for message ACKS -> {total_msg_send_time} (ms)')

    print(f'Total run time -> {total_time} (ms)')

def main():

    run_times = [10]

    it = 0
    
    while (it < 3):
        print(f'Trial Run #{it+1}')
        for seconds in run_times:
            sequential_execute(seconds)
            print('-----')
        it += 1

    """
    batch_size = [50,100,250,500,1000]

    for amount in batch_size:
                batch_send(sock, amount)
                print('-----')
    """
    sock = open_tcp()
    send_message(sock, 'F')
    close_tcp(sock)

    end_mysql_connection()

    
if __name__ == '__main__':
    main()