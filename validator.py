import json
import os
import time
import uuid
from database_manager import DatabaseManager
from urllib.parse import urlparse, unquote
from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobServiceClient
from inference_sdk import InferenceHTTPClient

# Configurações do Azure Blob Storage
connection_string_blob = os.getenv("CONNECTION_BLOB")
container_name = "validation-image-container"

# Configurações do Azure Service Bus
connection_string_bus_listen = os.getenv("CONNECTION_BUS_LISTEN")
queue_name = "validation-appointment"
 
CLIENT = InferenceHTTPClient(
    api_url="https://serverless.roboflow.com",
    api_key=os.getenv("ROBOFLOW_API_KEY")
)

# Função auxiliar para extrair o caminho do blob a partir da URL
def extract_blob_path_from_url(url):
    parsed_url = urlparse(url)
    return unquote(parsed_url.path.split("/", 2)[2])  # Pega o caminho após o nome do container


# Função para baixar as imagens do Azure Blob Storage para uma pasta local do projeto
def search_images(imgUrlInitial, imgUrlFinal):
    print("🔍 Função search_images() iniciada.")

    # Criar um cliente de serviço para acessar o Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)

    # Caminho da pasta temp na raiz do projeto
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")
    os.makedirs(temp_dir, exist_ok=True)
    print(f"📂 Diretório temporário (local) criado: {temp_dir}")

    def download_blob(img_url, temp_path):
        blob_path = extract_blob_path_from_url(img_url)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        with open(temp_path, "wb") as f:
            blob_data = blob_client.download_blob()
            f.write(blob_data.readall())
            print(f"📸 Imagem {blob_path} salva em {temp_path}")

    unique_id = str(uuid.uuid4())
    temp_img_initial_path = os.path.join(temp_dir, f"imagem_inicial_{unique_id}.png")
    temp_img_final_path = os.path.join(temp_dir, f"imagem_final_{unique_id}.png")

    download_blob(imgUrlInitial, temp_img_initial_path)
    download_blob(imgUrlFinal, temp_img_final_path)

    return [temp_img_initial_path, temp_img_final_path]


def validate_images(pathImgInitial, pathImgFinal, classInitial, classFinal):
    print("🧠 Função validate_images() iniciada.")
    
    result1 = CLIENT.infer(pathImgInitial, model_id="aparelho-dentario/2")
    result1 = result1.get("predictions", [{}])[0].get("class")
    result2 = CLIENT.infer(pathImgFinal, model_id="aparelho-dentario/2")
    result2 = result2.get("predictions", [{}])[0].get("class")
    
    print(f"  Classe identificada na primeira img: {result1}")
    print(f"  Classe identificada na segunda img: {result2}")
    
    result1 = formatar_classe(result1)
    result2 = formatar_classe(result2)
    
    print(f"  Classe identificada na primeira img: {result1}")
    print(f"  Classe identificada na segunda img: {result2}")
    
    if result1 == classInitial and result2 == classFinal:
        print("✅ Validação concluída com sucesso!")
        return True
    else:  
        print("❌ Validação falhou!")
        return False


def _atualizar_status_validacao(idAppointment, novo_status_id):
    """Função interna para atualizar o status da validação no banco de dados."""
    print(f"⚙️ Iniciando atualização do status para {idAppointment} com status ID {novo_status_id}.")
    db_manager = DatabaseManager()
    try:
        queryGetAppointment = """SELECT PROCEDURE_VALIDATION_ID FROM tb_appointment WHERE id = ?"""
        db_manager.execute_query(queryGetAppointment, (idAppointment,))
        result = db_manager.cursor.fetchone()

        if result is None:
            print(f"❌ Nenhuma validação encontrada para o agendamento {idAppointment}")
            return False

        idProcedureValidation = result[0]
        queryUpdateStatus = """UPDATE TB_PROCEDURE_VALIDATION SET PROCEDURE_STATUS_ID = ? WHERE id = ?"""
        db_manager.execute_query(queryUpdateStatus, (novo_status_id, idProcedureValidation,))
        print(f"✅ Status da consulta {idAppointment} atualizado para o status ID {novo_status_id} com sucesso.")
        return True
    finally:
        db_manager.close()

def valida_consulta_banco(idAppointment):
    """Valida uma consulta bancária, definindo o status como 'Aprovado sem Irregularidades'."""
    print("🗂️ Função valida_consulta_banco() iniciada.")
    _atualizar_status_validacao(idAppointment, 2)

def reanalise_encaminha_validacao(idAppointment):
    """Reanalisa e encaminha uma validação, definindo o status como 'Em Reanálise'."""
    print("🙋 Função reanalise_encaminha_validacao() iniciada.")
    _atualizar_status_validacao(idAppointment, 3)


def map_to_dict(mensagem_raw):
    try:
        msg_dict = json.loads(mensagem_raw)
        msg_data = {
            'idAppointment': msg_dict['idAppointment'],
            'imgUrlInitial': msg_dict['imgUrlInitial'],
            'imgUrlFinal': msg_dict['imgUrlFinal'],
            'classInitial': msg_dict['classInitial'],
            'classFinal': msg_dict['classFinal']
        }
        print("🧾 Dicionário montado:", msg_data)
        return msg_data
    except Exception as e:
        print(f"❌ Erro ao mapear mensagem: {e}")
        return None


def processar_mensagem(mensagem):
    print(f"📥 Processando mensagem: {mensagem}")
    msg_data = map_to_dict(mensagem)
    if not msg_data:
        print("❌ Falha ao processar a mensagem. Abortando...")
        return

    # Baixar as imagens
    pathImages = search_images(msg_data['imgUrlInitial'], msg_data['imgUrlFinal'])
    
    classesExpected = [msg_data['classInitial'], msg_data['classFinal']]

    # Validar as imagens
    validado = validate_images(pathImages[0], pathImages[1], classesExpected[0], classesExpected[1])

    if validado:
        valida_consulta_banco(msg_data['idAppointment'])
    else:
        reanalise_encaminha_validacao(msg_data['idAppointment'])

    # 🧹 Deletar as imagens após o uso
    for caminho in pathImages:
        if os.path.exists(caminho):
            os.remove(caminho)
            print(f"🗑️ Imagem deletada: {caminho}")

    print("✅ Processamento finalizado.\n")


def receive_message_from_queue():
    servicebus_client = ServiceBusClient.from_connection_string(connection_string_bus_listen)
    
    receiver = servicebus_client.get_queue_receiver(
        queue_name=queue_name  # até 5 minutos, por exemplo
    )


    try:
        print("🚀 Iniciando recepção de mensagens...\n")
        while True:
            mensagens = receiver.receive_messages(max_message_count=5, max_wait_time=5)

            if not mensagens:
                print("⏳ Nenhuma mensagem recebida. Aguardando...\n")
            else:
                for msg in mensagens:
                    processar_mensagem(str(msg))
                    receiver.complete_message(msg)

            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\n🛑 Execução interrompida pelo usuário.")
    finally:
        receiver.close()
        servicebus_client.close()
        print("🔒 Conexão fechada com a fila.")

def formatar_classe(classe: str) -> str:
    return classe.replace("-", "").replace(" ", "_").upper()

if __name__ == "__main__":
    receive_message_from_queue()
