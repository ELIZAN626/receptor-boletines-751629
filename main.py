import os
import boto3
import json
import uuid
import time

# configuracion
cola_url = os.getenv("SQS_URL")
sns_arn = os.getenv("SNS_ARN")
tabla_db = os.getenv("DYNAMO_TABLE")
region = os.getenv("AWS_REGION", "us-east-1")

# clientes de aws
sqs = boto3.client("sqs", region_name=region)
dynamo = boto3.resource("dynamodb", region_name=region)
sns = boto3.client("sns", region_name=region)
tabla = dynamo.Table(tabla_db)

def procesar():
    print("receptor iniciado y escuchando...")
    while True:
        try:
            # obtener mensaje de sqs
            # wait time en 20 para hacer long polling y ahorrar costos
            res = sqs.receive_message(
                QueueUrl=cola_url, 
                MaxNumberOfMessages=1, 
                WaitTimeSeconds=20
            )
            
            if "Messages" in res:
                for msg in res["Messages"]:
                    print("mensaje recibido, procesando...")
                    body = json.loads(msg["Body"])
                    
                    # generar id unico para el boletin
                    bid = str(uuid.uuid4())
                    
                    # guardar en base de datos dynamo
                    # se usan los campos exactos del json recibido
                    tabla.put_item(Item={
                        "boletin_id": bid,
                        "correo": body["correo"],
                        "mensaje": body["mensaje"],
                        "url_imagen": body["url_imagen"],
                        "leido": False
                    })
                    
                    # enviar notificacion sns
                    # NOTA la parte de mostrador-url siempre debemos cambiarla por la ip publica del servicio-mostrador

                    link_ver = f"http://mostrador-url/boletines/{bid}?correo={body['correo']}"
                    sns.publish(
                        TopicArn=sns_arn,
                        Message=f"nuevo boletin generado. puedes verlo aqui: {link_ver}",
                        Subject="nuevo boletin disponible"
                    )
                    
                    # borrar de la cola para que no se repita
                    sqs.delete_message(
                        QueueUrl=cola_url, 
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    print(f"proceso terminado para id: {bid}")
            
        except Exception as e:
            print(f"error en el procesamiento: {e}")
            
        # pausa breve para no saturar
        time.sleep(1)

if __name__ == "__main__":
    procesar()