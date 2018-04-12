package com.ai.renzq.flume.sink.es;

import com.ai.renzq.flume.sink.es.client.BulkProcessorBuilder;
import com.ai.renzq.flume.sink.es.client.ElasticSearchClientBuilder;
import com.ai.renzq.flume.sink.es.index_builder.IIndexBuilder;
import com.ai.renzq.flume.sink.es.serializer.ISerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ai.renzq.flume.sink.es.ElasticSearchSinkConstants.*;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/9 17:04
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/9 17:04
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class ElasticSearchSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSink.class);

    private SinkCounter sinkCounter;
    private String serverAddresses;

    private ISerializer serializer;
    private IIndexBuilder indexBuilder;

    private TransportClient client;
    BulkProcessor bulkProcessor;

    @VisibleForTesting
    public BulkProcessor getBulkProcessor(){
        return this.bulkProcessor;
    }

    @Override
    public void configure(Context context) {


        //TODO default value should be saved in the contant class.
            String[] hosts = getHosts(context);
            if(ArrayUtils.isNotEmpty(hosts)) {
                client = new ElasticSearchClientBuilder(
                    context.getString(ES_CLUSTER_NAME, "elasticsearch"), hosts)
                    .setTransportSniff(context.getBoolean(
                            ES_TRANSPORT_SNIFF, DEFAULT_ES_TRANSPORT_SNIFF))
                    .setIgnoreClusterName(context.getBoolean(
                            ElasticSearchSinkConstants.ES_IGNORE_CLUSTER_NAME, DEFAULT_ES_IGNORE_CLUSTER_NAME))
                    .setTransportPingTimeout(Utils.getTimeValue(context.getString(
                            ES_TRANSPORT_PING_TIMEOUT), DEFAULT_ES_TRANSPORT_PING_TIMEOUT))
                    .setNodeSamplerInterval(Utils.getTimeValue(context.getString(
                            ES_TRANSPORT_NODE_SAMPLER_INTERVAL), DEFAULT_ES_TRANSPORT_NODE_SAMPLER_INTERVAL))
                    .build();
                buildIndexBuilder(context);
                buildSerializer(context);

                bulkProcessor = new BulkProcessorBuilder().buildBulkProcessor(context, client);
            } else {
                logger.error("Could not create transport client, No host exist");
            }
    }

    @Override
    public synchronized void start() {
        logger.debug("=============es_sink_start==============");
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.debug("==============es_sink_close==============");
        try {
            if (bulkProcessor != null) {
                bulkProcessor.flush();
                bulkProcessor.close();
            }
        }catch(Throwable t){
            logger.error("close bluk error:" + t.getMessage(), t);
        }
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status result;

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();
        try{
            event = ch.take();
            byte[] eventBody;
            if(event != null) {
                eventBody = event.getBody();
                String body = new String(eventBody, Charsets.UTF_8);

                if (!Strings.isNullOrEmpty(body)) {
                    logger.debug("start to sink event [{}].", body);
                    String index = this.indexBuilder.getIndex(event);
                    String type = this.indexBuilder.getType(event);
                    String id = this.indexBuilder.getId(event);

                    XContentBuilder xContentBuilder = serializer.serialize(event);
                    if(xContentBuilder != null) {
                        if (!Strings.isNullOrEmpty(id)) {
                            /*If there is the ID, using update*/
                            bulkProcessor.add(new UpdateRequest(index, type, id).doc(xContentBuilder).upsert(xContentBuilder));
                        } else {
                            bulkProcessor.add(new IndexRequest(index, type)
                                    .source(xContentBuilder));
                        }
                    } else {
                        logger.error("Could not serialize the event body [{}] for index [{}], type[{}] and id [{}] ",
                                new Object[]{body, index, type, id});
                    }
                    logger.debug("sink event [{}] successfully.", body);
                }

            }

            result = Status.READY;
            txn.commit();
        } catch (Throwable e) {

            txn.rollback();

            result = Status.BACKOFF;
            // re-throw all Errors
            if (e instanceof Error) {
                logger.error("[ERROR][process]"+ e.toString());
                Throwables.propagate(e);
            }

            logger.debug("[ERROR][process]"+ e.toString(), e);
        }


        txn.close();
        return result;
    }


    private String[] getHosts(Context context) {
        String[] hosts = null;
        if (StringUtils.isNotBlank(context.getString(ES_HOSTS))) {
            hosts = context.getString(ES_HOSTS).split(",");
        }
        return hosts;
    }

    private void buildIndexBuilder(Context context) {
        String indexBuilderClass = DEFAULT_ES_INDEX_BUILDER;
        if (StringUtils.isNotBlank(context.getString(ES_INDEX_BUILDER))) {
            indexBuilderClass = context.getString(ES_INDEX_BUILDER);
        }
        this.indexBuilder = instantiateClass(indexBuilderClass);
        if (this.indexBuilder != null) {
            this.indexBuilder.configure(context);
        }
    }

    private void buildSerializer(Context context) {
        String serializerClass = DEFAULT_ES_SERIALIZER;
        if (StringUtils.isNotEmpty(context.getString(ES_SERIALIZER))) {
            serializerClass = context.getString(ES_SERIALIZER);
        }
        this.serializer = instantiateClass(serializerClass);
        if(this.serializer != null) {
            this.serializer.configure(context);
        }
    }

    private <T> T instantiateClass(String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> aClass = (Class<T>) Class.forName(className);
            return aClass.newInstance();
        } catch (Exception e) {
            logger.error("Could not instantiate class " + className, e);
            Throwables.propagate(e);
            return null;
        }
    }


}
