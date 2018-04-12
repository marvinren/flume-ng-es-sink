package com.ai.renzq.flume.sink.es;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/9 17:21
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/9 17:21
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class ElasticSearchSinkConstants {

    public static final String COMMA=",";
    public static final String COLONS = ":";

    /*elasticsearch config name in conf file*/
    public static final String ES_HOSTS = "es.client.hosts";

    public static final String ES_CLUSTER_NAME = "es.cluster.name";
    public static final String DEFAULT_ES_CLUSTER_NAME = "elasticsearch";
    public static final String ES_TRANSPORT_SNIFF = "es.transport.sniff";
    public static final boolean DEFAULT_ES_TRANSPORT_SNIFF = false;
    public static final String ES_IGNORE_CLUSTER_NAME = "es.client.transport.ignore_cluster_name";
    public static final boolean DEFAULT_ES_IGNORE_CLUSTER_NAME = false;
    public static final String ES_TRANSPORT_PING_TIMEOUT = "es.client.transport.ping_timeout";
    public static final String DEFAULT_ES_TRANSPORT_PING_TIMEOUT = "5s";
    public static final String ES_TRANSPORT_NODE_SAMPLER_INTERVAL = "es.client.transport.nodes_sampler_interval";
    public static final String DEFAULT_ES_TRANSPORT_NODE_SAMPLER_INTERVAL = "5s";

    public static final String ES_BULK_ACTIONS = "es.bulkActions";
    public static final Integer DEFAULT_ES_BULK_ACTIONS = 1000;
    public static final String ES_BULK_SIZE = "es.bulkSize";
    public static final Integer DEFAULT_ES_BULK_SIZE = 5;
    public static final String ES_CONCURRENT_REQUEST = "es.concurrent.request";
    public static final Integer DEFAULT_ES_CONCURRENT_REQUEST = 1;
    public static final String ES_FLUSH_INTERVAL_TIME = "es.flush.interval.time";
    public static final Integer DEFAULT_ES_FLUSH_INTERVAL_TIME = 10;
    public static final String ES_BACKOFF_POLICY_TIME_INTERVAL = "es.backoff.policy.time.interval";
    public static final Integer DEFAULT_ES_BACKOFF_POLICY_START_DELAY = 100;
    public static final String ES_BACKOFF_POLICY_RETRIES = "es.backoff.policy.retries";
    public static final Integer DEFAULT_ES_BACKOFF_POLICY_RETRIES = 3;

    public static final String ES_SERIALIZER="es.serializer";
    public static final String DEFAULT_ES_SERIALIZER="com.ai.renzq.flume.sink.es.serializer.JsonSerializer";
    public static final String ES_SERIALIZER_CSV_FIELD = "es.serializer.csv.fields";
    public static final String ES_CSV_DELIMITER = "es.serializer.cvs.delmiter";
    public static final String DEFAULT_ES_CSV_DELIMITER = ",";


    /*elasticsearch transport client settings' name*/
    public static final String ES_ORIGIN_CLUSTER_NAME = "cluster.name";
    public static final String ES_ORIGIN_TRANSPORT_SNIFF = "client.transport.sniff";
    public static final String ES_ORIGIN_INGORE_CLUSTER_NAME = "client.transport.ignore_cluster_name";
    public static final String ES_ORIGIN_PING_TIMEOUT = "client.transport.ping_timeout";
    public static final String ES_ORIGIN_NODE_SAMPLER_INTERVAL = "client.transport.nodes_sampler_interval";

    /*Index configure*/
    public static final String ES_INDEX_DEFAULT_NAME="es.index.default.name";
    public static final String ES_INDEX_DEFAULT_TYPE="es.index.default.type";
    public static final String ES_INDEX_ID_FIELD="es.index.id";
    public static final String ES_INDEX_BUILDER="es.index.builder";
    public static final String DEFAULT_ES_INDEX_BUILDER="com.ai.renzq.flume.sink.es.index_builder.StaticIndexBuilder";

    /*DEFAULT configure*/
    public static final int DEFAULT_PORT = 9300;
    public static final String DEFAULT_ES_INDEX = "elasticsearch";
    public static final String DEFAULT_ES_TYPE = "doc";

}
