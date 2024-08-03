import configparser
from pyspark import SparkConf


def get_app_config(env):
    """ loading the application configs in python dictionary"""
    config = configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf
    
# 
def get_pyspark_config(env):
    """loading the pyspark configs and creating a spark conf object"""
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf