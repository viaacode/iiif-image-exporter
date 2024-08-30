# System imports
import functools
import os
# Internal imports
from viaa.configuration import ConfigParser
from mediahaven import MediaHaven
from mediahaven.resources.base_resource import MediaHavenPageObject
from mediahaven.mediahaven import MediaHavenException
from mediahaven.oauth2 import ROPCGrant

# External imports
import pika

config_parser = ConfigParser()

client_id = config_parser.app_cfg["mediahaven"]["client"]
client_secret = config_parser.app_cfg["mediahaven"]["secret"]
user = config_parser.app_cfg["mediahaven"]["username"]
password = config_parser.app_cfg["mediahaven"]["password"]
url = config_parser.app_cfg["mediahaven"]["host"]
grant = ROPCGrant(url, client_id, client_secret)
grant.request_token(user, password)
mediahaven_client = MediaHaven(url, grant)

class Consumer:
    def __init__(self) -> None:
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg

        # Rabbit config
        rabbit_config = self.config["rabbitmq"]
        rabbit_credentials = pika.PlainCredentials(rabbit_config["username"], rabbit_config["password"])
        self.rabbit_parameters = pika.ConnectionParameters(rabbit_config["host"], credentials=rabbit_credentials)
        self.rabbit_queue = rabbit_config["queue"]
        
    def remove_file(self, file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print(f"The file {file_path} does not exist")
        
    def on_message(self, chan, method_frame, header_frame, body, userdata=None):
        print("Doing something")
        # Start image processing workflow

        # Convert string to object
        msg = eval(body.decode())
        method = msg["action"]

        if method == "create":
            visibility = 'public' if 'public' in msg['path'] else 'restricted'
            fragment_id = msg["fragment_id"]
            
            export_dict = {
                "Records": [{
                    "RecordId": fragment_id
                }],
                "ExportLocationId": config_parser.app_cfg["mediahaven"]["export_location_id"],
                "Reason": "IIIF image processing.",
                "Combine": "Zip",
                "DestinationPath": f"{visibility}/{msg['dcterms_format']}"
            }
            
            try:
                mediahaven_client._post("exports", json=export_dict)
                print(f"MH export triggered: {export_dict}")
            except Exception as e:
                print(f"MH export failed: {e}")
        elif method == "delete":
            visibility = 'public' if 'public' in msg['path'] else 'restricted'
            or_id = msg['OR-id']
            fragment_id = msg["fragment_id"]
            characters = fragment_id[:2]
            file_to_delete = '/export/images/'
            + visibility
            + "/"
            + or_id
            + "/"
            + characters
            + "/"
            + fragment_id
            + ".jph"
            print(f"deleting {file_to_delete}")
            self.remove_file(file_to_delete)

        print(f'fragment_id: {msg["fragment_id"]}')
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
        
    def main(self) -> None:
        print(f"Start consuming:")
        connection = pika.BlockingConnection(self.rabbit_parameters)

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        on_message_callback = functools.partial(self.on_message, userdata="on_message_userdata")
        channel.basic_consume(self.rabbit_queue, on_message_callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        connection.close()
