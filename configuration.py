from os import path

DATA_ROOT = "/home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on"
ROOT_URL = "file://" + DATA_ROOT

def data_url(filename):
    return path.join(ROOT_URL, filename)

def data_filename(filename):
    return path.join(DATA_ROOT, filename)
