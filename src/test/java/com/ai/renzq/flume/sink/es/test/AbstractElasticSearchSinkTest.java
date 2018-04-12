package com.ai.renzq.flume.sink.es.test;
import com.ai.renzq.flume.sink.es.ElasticSearchSink;
import com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import javafx.scene.NodeBuilder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.test
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/9 17:20
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/9 17:20
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class AbstractElasticSearchSinkTest {

    Channel bindAndStartChannel(ElasticSearchSink fixture) {
        // Configure the channel
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, new Context());

        // Wire them together
        fixture.setChannel(channel);
        fixture.start();
        return channel;
    }


}
