from pyspark import SparkConf, SparkContext
import numpy as np
import argparse
import cv2
import pika
import json

## CONSTANTS

class MicroObject(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

APP_NAME = "My Spark Application"
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--id", required=True,
	help="id from disk")
ap.add_argument("-p", "--prototxt", required=True,
	help="path to Caffe 'deploy' prototxt file")
ap.add_argument("-m", "--model", required=True,
	help="path to Caffe pre-trained model")
ap.add_argument("-c", "--confidence", type=float, default=0.2,
	help="minimum probability to filter weak detections")
    
args = vars(ap.parse_args())

ret = dict()
CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
"bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
"dog", "horse", "motorbike", "person", "pottedplant", "sheep",
"sofa", "train", "tvmonitor"]

for c in CLASSES:
    ret[c] = 0

    
COLORS = np.random.uniform(0, 255, size=(len(CLASSES), 3))


net = cv2.dnn.readNetFromCaffe(args["prototxt"], args["model"])
## Main functionality

def recognize(p):
    try:
        print p
        image = cv2.imread("C:\\Vids\\" + args["id"] + "\\" + str(p) +".jpg")
        (h, w) = image.shape[:2]
        blob = cv2.dnn.blobFromImage(cv2.resize(image, (300, 300)), 0.007843, (300, 300), 127.5)

        # pass the blob through the network and obtain the detections and
        # predictions
        print("[INFO] computing object detections...")
        net.setInput(blob)
        detections = net.forward()

        # loop over the detections
        for i in np.arange(0, detections.shape[2]):

            confidence = detections[0, 0, i, 2]
            if confidence > args["confidence"]:
                idx = int(detections[0, 0, i, 1])
                box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
                #(startX, startY, endX, endY) = box.astype("int")

                # display the prediction
                label = "{}: {:.2f}%".format(CLASSES[idx], confidence * 100)
                ret[CLASSES[idx]] = ret[CLASSES[idx]] + 1

                print("[INFO] {}".format(label))
                #cv2.rectangle(image, (startX, startY), (endX, endY),
	            #    COLORS[idx], 2)
                #y = startY - 15 if startY - 15 > 15 else startY + 15
                #cv2.putText(image, label, (startX, y),
	            #    cv2.FONT_HERSHEY_SIMPLEX, 0.5, COLORS[idx], 2)
        pass
    except  exc:
        print exc
    return 0



    


def main(sc, tries):
    print sc
    print tries
    #rdd = sc.parallelize(xrange(0, tries))
    
    for i in xrange(0, tries):
        recognize(i)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='done_items', durable=True)

    o = MicroObject(Guid = args["id"], Occurences = ret)

    message = o.toJSON()
    channel.basic_publish(exchange='',
                          routing_key='done_items',
                          body=message,
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))
    print(" [x] Sent %r" % message)
    connection.close()
    #rdd.foreach(recognize)
    #rdd.saveAsTextFile("C:\\Vids\\" + args["id"] + "\\res.txt")



if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.set("spark.network.timeout", "600s")
    conf = conf.set("spark.driver.maxResultSize", "2g")
    conf = conf.setMaster("local[*]")
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    sc   = SparkContext(conf=conf)
    # Execute Main functionality
    
    number_of_tries = int(open("C:\\Vids\\" + args["id"] + "\\info.ini", "r").next())

    main(sc,number_of_tries)