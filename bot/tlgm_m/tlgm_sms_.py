import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def enviar_mensaje_telegram(token, chat_id, mensaje):
    try:
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        os.environ["TELEGRAM_CHAT_ID"] = chat_id
        TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": mensaje,
            "parse_mode": "Markdown"
        }
        session = requests.Session()
        retries = Retry(total=100, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.post(url, json=payload)
        response_data = response.json()
        if response.status_code == 200 and response_data["ok"]:
            print("Mensaje enviado con éxito.")
            return response_data["result"]["message_id"]  # Devuelve el ID del mensaje enviado
        else:
            print(f"Error al enviar el mensaje: {response_data}")
    except Exception as e:
        print(f"Un error inesperado ocurrió: {e}")
        return None

def enviar_imagen_telegram(token, chat_id, ruta_imagen, caption=None):
    try:
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        os.environ["TELEGRAM_CHAT_ID"] = chat_id
        TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

        url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
        with open(ruta_imagen, 'rb') as imagen:
            files = {'photo': imagen}
            data = {'chat_id': CHAT_ID, 'caption': caption} if caption else {'chat_id': CHAT_ID}
            session = requests.Session()
            retries = Retry(total=100, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
            session.mount("https://", HTTPAdapter(max_retries=retries))
            response = session.post(url, files=files, data=data)
            response_data = response.json()
            if response.status_code == 200 and response_data["ok"]:
                print("Imagen enviada con éxito.")
                return True
            else:
                print(f"Error al enviar la imagen: {response_data}")
                return False
    except FileNotFoundError:
        print(f"Error: No se encontró la imagen en la ruta: {ruta_imagen}")
        return False
    except Exception as e:
        print(f"Un error inesperado ocurrió: {e}")
        return False


def responder_a_mensaje(token, chat_id, message_id, mensaje_respuesta):
    try:
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        os.environ["TELEGRAM_CHAT_ID"] = chat_id
        TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": mensaje_respuesta,
            "reply_to_message_id": message_id,
            "parse_mode": "Markdown"
        }
        session = requests.Session()
        retries = Retry(total=100, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.post(url, json=payload)
        response_data = response.json()
        if response.status_code == 200 and response_data["ok"]:
            print(f"Respondiendo al mensaje con ID: {message_id}")
        else:
            print(f"Error al responder el mensaje: {response_data}")
    except Exception as e:
        print(f"Un error inesperado ocurrió: {e}")


def responder_con_mensaje_e_imagen(token, chat_id, message_id, ruta_imagen, caption=None):
    try:
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        os.environ["TELEGRAM_CHAT_ID"] = chat_id
        TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
        session = requests.Session()
        retries = Retry(total=100, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        # Enviar imagen de respuesta
        url_imagen = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
        with open(ruta_imagen, 'rb') as imagen:
            files = {'photo': imagen}
            data = {'chat_id': CHAT_ID, 'reply_to_message_id': message_id}
            if caption:
                data['caption'] = caption
            response_imagen = session.post(url_imagen, files=files, data=data)
            response_data_imagen = response_imagen.json()
            if response_imagen.status_code == 200 and response_data_imagen["ok"]:
                print(f"Respondiendo al mensaje con ID: {message_id} con imagen")
            else:
                print(f"Error al enviar la imagen: {response_data_imagen}")

    except FileNotFoundError:
        print(f"Error: No se encontró la imagen en la ruta: {ruta_imagen}")
    except Exception as e:
        print(f"Un error inesperado ocurrió: {e}")



def main():
    os.environ["TELEGRAM_BOT_TOKEN"] = "8097341221:AAGnrxhv4bHopzNIWkEOcDOiup_MRV5XiaU"
    os.environ["TELEGRAM_CHAT_ID"] = "-1002357427460"

    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    mensaje_a_enviar = "📊 Señal de Trading Activa 📊 \n\nMercado: Futuros \nActivo: GRASSSDT \nOperación: LONG \nEntrada: 2.995 \nTake Profit (TP): 3.10 \nApalancamiento sugerido: 5x \nModo: Aislado \n⏳ Hora de envío: 11:30AM\n\n⚠ IMPORTANTE: Las señales son resultado de un análisis técnico, pero no garantizan resultados.El uso de esta información es bajo su propio riesgo."
    message_id = enviar_mensaje_telegram(TOKEN, CHAT_ID, mensaje_a_enviar)
    
    if message_id:
        
        responder_a_mensaje(TOKEN, CHAT_ID, message_id, mensaje_a_enviar)
        caption = "Estamos on air SIUUUUUU, se logró @cryptomauf 😎"
        message_id = 32
        #responder_con_mensaje_e_imagen(TOKEN, CHAT_ID, message_id, 'output_report_bot.png', caption)

        ruta_imagen = "output.png"  # Asegúrate de que este archivo exista en el mismo directorio
        caption = "Esta es una imagen de prueba 27/09/2021"

        #(enviar_imagen_telegram(TOKEN, CHAT_ID, ruta_imagen, caption))

if __name__ == '__main__':
    main()

