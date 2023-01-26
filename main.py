import uuid

import psycopg2

import time

import paho.mqtt.client as mqtt

import json


################################ MQTT Service ################################################


app_id = "imredd@ttn"

access_key = "NNSXS.ZY5VXTSCFYLG7W4CBDYPS6APVCEXL42ZS6FVUWQ.JY3OS22UQ6OHWBFS3YZ2UBCD2FMCX4UECWIKEFX5UVSJRBJKHOYA"

SENSOR_NAME = "ale"


# messageJSON = json.loads(message)

# print(messageJSON['uplink_message']['decoded_payload']['analog_in_3'])

# The callback for when the client receives a CONNACK response from the server.

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and

    # reconnect then subscriptions will be renewed.

    # client.subscribe("v3/imredd@ttn/devices/sensor/up")

    client.subscribe("v3/imredd@ttn/devices/" + SENSOR_NAME + "/up")


# The callback for when a PUBLISH message is received from the server.

def on_message(client, userdata, msg):
    # print(msg.topic+" "+str(msg.payload))

    # Decode UTF-8 bytes to Unicode, and convert single quotes

    # to double quotes to make it valid JSON

    my_json = msg.payload.decode('utf8').replace("'", '"')

    print(my_json)

    print('- ' * 20)

    # Load the JSON to a Python list & dump it back out as formatted JSON

    data = json.loads(my_json)

    # s = json.dumps(data, indent=4, sort_keys=True)

    # print(s)

    uplinkmessage = data['uplink_message']

    if "decoded_payload" in uplinkmessage:  # if we have a message that includes the data, we will send the data to the databse

        print("Key exist in JSON data. Temperature: ")

        luminosity = uplinkmessage['decoded_payload']['luminosity_4']

        temperature = uplinkmessage['decoded_payload']['temperature_1']

        presence_digital = uplinkmessage['decoded_payload']['digital_in_6']

        humidity = uplinkmessage['decoded_payload']['relative_humidity_2']

        now = int(time.time())

        print(uplinkmessage['decoded_payload']['temperature_1'])

        db = psycopg2.connect(
            database="imredd",
            user="imredd",
            password="LVFfzkZ4kCpNQEwJtY1Mpf4wIPXwRRxg",
            host="dpg-cf8jgkpmbjss4md9k830-a.frankfurt-postgres.render.com",
            port="5432")

        # create a cursor to navigate in the database:

        cursor = db.cursor()

        query = "INSERT INTO sensordataproject (id, luminosity, temperature, presence_digital, humidity , timestamp) VALUES (%s,%s,%s,%s,%s,%s);"

        values = (str(uuid.uuid4()), float(luminosity), float(temperature), float(presence_digital), float(humidity), now)

        cursor.execute(query, values)

        db.commit()

        db.close()

        print("data Inserted")

    else:

        print("Key doesn't exist in JSON data")


client = mqtt.Client()

client.username_pw_set(app_id, access_key)

client.on_connect = on_connect

client.on_message = on_message

client.connect("eu1.cloud.thethings.network", 1883, 60)

# client.connect("localhost", 1883, 60)


# Blocking call that processes network traffic, dispatches callbacks and

# handles reconnecting.

# Other loop*() functions are available that give a threaded interface and a

# manual interface.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    client.loop_forever()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
