#!/usr/bin/python

import sys
import time
import urllib2
import json
import logging.handlers
import os

PREFIX = os.environ['PREFIX']
VT_UID = os.environ['VT_UID']
LOG_FILENAME = '/var/log/vitess/log.out'

# Set up a specific logger with our desired output level
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=1048576, backupCount=10)

logger.addHandler(handler)


def entry(method_name):
    logger.debug("In: " + method_name)


def leave(method_name):
    logger.debug("Out: " + method_name)


def get_url():
    if len(sys.argv) != 2:
        print 'Url is required as the argument'
        exit()
    return sys.argv[1]


def get_epoch_time():
    return int(time.time())


def get_json_data(url):
    return json.load(urllib2.urlopen(url))


def create_metric(epoch_time, metric_name, data, tag_list):
    method_name = "create_metric for " + metric_name
    entry(method_name)

    for key in data.keys():
        tags = extract_tags(key, '.', tag_list)
        if tags != "-1":
            publish_metric(epoch_time, metric_name, data[key], tags)

    leave(method_name)


def create_metric_histogram(epoch_time, metric_name, data, tag_list):
    method_name = "create_metric_histogram for " + metric_name
    entry(method_name)

    histogram_data = data['Histograms']
    for key in histogram_data.keys():
        tags = extract_tags(key, '.', tag_list)
        if tags != "-1":
            publish_metric(epoch_time, metric_name, histogram_data[key]['Count'], tags)

    leave(method_name)


def create_metric_avg(epoch_time, metric_name, data_time, data_count, tag_list):
    method_name = "create_metric for " + metric_name
    entry(method_name)

    for key in data_time.keys():
        tags = extract_tags(key, '.', tag_list)
        if tags != "-1":
            if data_count[key] != 0:
                publish_metric(epoch_time, metric_name, (data_time[key]/1000000.0)/data_count[key], tags)
            else:
                publish_metric(epoch_time, metric_name, 0.0, tags)

    leave(method_name)


def extract_tags(key, split_char, tag_list):
    tag_data = key.split(split_char)
    if len(tag_data) != len(tag_list):
        logger.error("extract_tags: Data not as expected for " + key + " tag list: "
                     + str(tag_list))
        return "-1"
    else:
        tags = {}
        i = 0
        for tag in tag_data:
            tags[tag_list[i]] = tag.replace(" ", "_")
            i += 1
        return tags


def publish_metric(epoch_time, metric_name, metric_value, tags):
    method_name = "publish_metric for " + metric_name
    entry(method_name)
    tag_str = ""
    if tags:
        total_keys = len(tags)
        i = 0
        for key in tags.keys():
            tag_str += key + "=" + tags[key]
            i += 1
            tag_str += " "
    tag_str += "uid=" + VT_UID
    if PREFIX.lower() in "none":
        print (str(epoch_time) + " " + metric_name + " " + str(metric_value) + " " + tag_str)
    else:
        print (str(epoch_time) + " " + PREFIX + "." + metric_name + " " + str(metric_value) + " " + tag_str)
    leave(method_name)
