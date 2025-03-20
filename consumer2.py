from azure.servicebus import ServiceBusClient, ServiceBusMessage
import time
import os

# Definindo a string de conexão e o nome da fila
connection_string = os.getenv("CONNECTION_BUS_SEND")
queue_name = "validation-appointment"


def receive_message_from_queue():
    # Criando o cliente ServiceBus
    servicebus_client = ServiceBusClient.from_connection_string(connection_string)

    # Obtendo o receptor da fila
    receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)

    try:
        # Iniciando a recepção das mensagens
        print("Initiating message reception...")
        while True:
            # Recebendo uma mensagem de cada vez
            received_msgs = receiver.receive_messages(max_message_count=1, max_wait_time=1)

            for msg in received_msgs:
                # Processando a mensagem
                print(f"Message received: {msg}")

                # Completar a mensagem após o processamento (confirmando a recepção)
                receiver.complete_message(msg)

            # Atraso de 1 segundo (similar ao sleep(1000) do código Java)
            time.sleep(1)

    except KeyboardInterrupt:
        print("Processamento interrompido pelo usuário.")
    finally:
        # Fechando o cliente quando terminar
        receiver.close()
        servicebus_client.close()


# Chamada da função para receber mensagens da fila
receive_message_from_queue()