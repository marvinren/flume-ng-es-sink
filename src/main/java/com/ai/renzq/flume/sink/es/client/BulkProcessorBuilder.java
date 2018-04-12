package com.ai.renzq.flume.sink.es.client;

import org.apache.flume.Context;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.ai.renzq.flume.sink.es.ElasticSearchSinkConstants.*;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.client
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/10 23:00
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/10 23:00
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class BulkProcessorBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BulkProcessorBuilder.class);

    private Context context;


    public BulkProcessor buildBulkProcessor(Context context, TransportClient client) {
        this.context = context;
        return this.build(client);
    }


    private BulkProcessor build(TransportClient client) {

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        logger.debug("Bulk Execution [" + executionId + "]\n" +
                                "No of actions " + request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        logger.debug("Bulk execution completed [" + executionId + "]\n" +
                                "Took (ms): " + response.getIngestTookInMillis() + "\n" +
                                "Failures: " + response.hasFailures() + "\n" +
                                "Failures Message: " + response.buildFailureMessage() + "\n" +
                                "Count: " + response.getItems().length);

                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        logger.error("Bulk execution failed [" + executionId + "]" +
                                failure.toString());
                        logger.error(failure.getMessage(), failure);
                    }
                })
                .setBulkActions(context.getInteger(ES_BULK_ACTIONS, DEFAULT_ES_BULK_ACTIONS))
                .setBulkSize(new ByteSizeValue(context.getInteger(ES_BULK_SIZE, DEFAULT_ES_BULK_SIZE), ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(context.getInteger(ES_FLUSH_INTERVAL_TIME, DEFAULT_ES_FLUSH_INTERVAL_TIME)))
                .setConcurrentRequests(context.getInteger(ES_CONCURRENT_REQUEST, DEFAULT_ES_CONCURRENT_REQUEST))
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(context.getInteger(ES_BACKOFF_POLICY_TIME_INTERVAL, DEFAULT_ES_BACKOFF_POLICY_START_DELAY)),
                                context.getInteger(ES_BACKOFF_POLICY_RETRIES, DEFAULT_ES_BACKOFF_POLICY_RETRIES)))
                .build();
        return bulkProcessor;
    }
}
