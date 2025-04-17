from flask import Flask, request, jsonify
from inference_sdk import InferenceHTTPClient
import os

app = Flask(__name__)

CLIENT = InferenceHTTPClient(
    api_url="https://detect.roboflow.com",
    api_key=os.getenv("ROBOFLOW_API_KEY")
)



@app.route('/compare', methods=['POST'])
def compare_images():
    try:
        # Recebe os nomes dos arquivos de imagem
        image1 = request.files.get('image1')
        image2 = request.files.get('image2')

        if not image1 or not image2:
            return jsonify({"error": "Por favor, envie duas imagens."}), 400

        # Salva temporariamente as imagens
        image1_path = 'temp/image1.jpg'
        image1.save(image1_path)
        image2_path = 'temp/image2.jpg'
        image2.save(image2_path)

        # Realiza inferência para as duas imagens
        result1 = CLIENT.infer(image1_path, model_id="aparelho-dentario/2")
        result1 = result1.get('predictions', [{}])[0]
        result2 = CLIENT.infer(image2_path, model_id="aparelho-dentario/2")
        result2 = result2.get('predictions', [{}])[0]

        return jsonify({
            "classe_identificada_inicio": result1,
            "classe_identificada_fim": result2
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
