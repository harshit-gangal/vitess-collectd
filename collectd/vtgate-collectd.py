#!/usr/bin/python

import util


TAG_LIST_1 = ['keyspace', 'shard', 'type']
TAG_LIST_2 = ['type']
TAG_LIST_3 = ['method', 'keyspace', 'shard', 'type']
TAG_LIST_4 = ['method', 'keyspace', 'type']


def process_data(json_data):
    epoch_time = util.get_epoch_time()

    util.create_metric(epoch_time, "vitess.healthcheckConnections", json_data['HealthcheckConnections']
                       , TAG_LIST_1)

    util.create_metric(epoch_time, "vitess.healthcheckErrors", json_data['HealthcheckErrors']
                       , TAG_LIST_1)

    util.create_metric(epoch_time, "vitess.vtgateApiErrorCounts", json_data['VtgateApiErrorCounts']
                       , TAG_LIST_4)

    util.create_metric(epoch_time, "vitess.vtgateApiRowsReturned", json_data['VtgateApiRowsReturned']
                       , TAG_LIST_4)

    util.create_metric(epoch_time, "vitess.vtgateInfoErrorCounts", json_data['VtgateInfoErrorCounts']
                       , TAG_LIST_2)

    util.create_metric(epoch_time, "vitess.vtgateInternalErrorCounts"
                       , json_data['VtgateInternalErrorCounts'], TAG_LIST_2)

    util.create_metric(epoch_time, "vitess.vttabletCallErrorCount", json_data['VttabletCallErrorCount']
                       , TAG_LIST_3)

    util.publish_metric(epoch_time, "vitess.vtgateApi.totalCount", json_data['VtgateApi']['TotalCount']
                        , None)

    util.create_metric_histogram(epoch_time, "vitess.vtgateApi.count", json_data['VtgateApi']
                                 , TAG_LIST_4)

    util.publish_metric(epoch_time, "vitess.vttabletCall.totalCount"
                        , json_data['VttabletCall']['TotalCount'], None)

    util.create_metric_histogram(epoch_time, "vitess.vttabletCall.count", json_data['VttabletCall']
                                 , TAG_LIST_3)


def main():
    url = util.get_url()
    json_data = util.get_json_data(url)
    process_data(json_data)


if __name__ == '__main__':
    main()
