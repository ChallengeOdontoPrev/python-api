import json
import os
import time
import uuid
from urllib.parse import urlparse, unquote

from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobServiceClient

# Configurações do Azure Blob Storage
connection_string_blob = os.getenv("CONNECTION_BLOB")
container_name = "validation-image-container"

# Configurações do Azure Service Bus
connection_string_bus_listen = os.getenv("CONNECTION_BUS_LISTEN")
queue_name = "validation-appointment"


# Função auxiliar para extrair o caminho do blob a partir da URL
def extract_blob_path_from_url(url):
    parsed_url = urlparse(url)
    return unquote(parsed_url.path.split("/", 2)[2])  # Pega o caminho após o nome do container


# Função para baixar as imagens do Azure Blob Storage para uma pasta local do projeto
def buscar_imagens(imgUrlInitial, imgUrlFinal):
    print("🔍 Função buscar_imagens() iniciada.")

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


def validar_imagens(pathImgInitial, pathImgFinal):
    print("🧠 Função validar_imagens() iniciada.")
    print(f"  Validando imagem inicial em: {pathImgInitial}")
    print(f"  Validando imagem final em: {pathImgFinal}")
    return True


def atualizar_status_banco():
    print("🗂️ Função atualizar_status_banco() iniciada.")


def encaminhar_para_validacao_humana():
    print("🙋 Função encaminhar_para_validacao_humana() iniciada.")


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
    caminhos_imagens = buscar_imagens(msg_data['imgUrlInitial'], msg_data['imgUrlFinal'])

    # Validar as imagens
    validado = validar_imagens(caminhos_imagens[0], caminhos_imagens[1])

    if validado:
        atualizar_status_banco()
    else:
        encaminhar_para_validacao_humana()

    # 🧹 Deletar as imagens após o uso
    for caminho in caminhos_imagens:
        if os.path.exists(caminho):
            os.remove(caminho)
            print(f"🗑️ Imagem deletada: {caminho}")

    print("✅ Processamento finalizado.\n")


def receive_message_from_queue():
    servicebus_client = ServiceBusClient.from_connection_string(connection_string_bus_listen)
    receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)

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


if __name__ == "__main__":
    receive_message_from_queue()
