import json
import logging
from google.cloud import pubsub_v1

from model.MessageAppointmentDTO import MessageAppointmentValidationDTO

# CONFIG DO PROJETO
PROJECT_ID = ""
SUBSCRIPTION_ID = ""

# CLIENT DO SUBSCRIBER
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


def process_appointment(data: dict):
    """
        Função para processar a mensagem recebida.
        validar, persistir no DB, chamar outra API, etc.
    """
    try:
        # Valida e converte o dicionário para uma instância do modelo
        appointment = MessageAppointmentValidationDTO(**data)
        logging.info(f"Processando agendamento: {appointment}")
        # Exemplo: Acessando os dados
        logging.info(f"ID do agendamento: {appointment.idAppointment}")
        logging.info(f"Imagem Inicial: {appointment.imgUrlInitial}")
        logging.info(f"Imagem Final: {appointment.imgUrlFinal}")
        logging.info(f"Classe Inicial: {appointment.classInitial}")
        logging.info(f"Classe Final: {appointment.classFinal}")
        # TODO: Adicione aqui a lógica de processamento que desejar.
    except Exception as e:
        logging.error(f"Erro ao validar/processar a mensagem: {e}")
        raise


def callback(message):
    try:
        msg_str = message.data.decode('utf-8')
        logging.info(f"Mensagem recebida: {msg_str}")
        msg_data = json.loads(msg_str)
        process_appointment(msg_data)
        message.ack()
    except Exception as e:
        logging.error(f"Erro no callback: {e}")
        message.nack()


def start_consumer():
    """
        Função que inicia o subscriber do Pub/Sub.
        Essa função deve ser executada em background.
    """
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logging.info(f"Consumidor iniciando na subscrição: {subscription_path}")

    try:
        streaming_pull_future.result()
    except Exception as e:
        logging.error(f"Erro ao consumir mensagens: {e}")
        streaming_pull_future.cancel()