package com.ai.renzq.flume.sink.es.test;

import com.ai.renzq.flume.sink.es.ElasticSearchSink;
import org.apache.flume.*;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.client.Requests;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.test
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/9 17:25
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/9 17:25
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class ElasticSearchSinksTest extends AbstractElasticSearchSinkTest{

    private ElasticSearchSink fixture;
    private Context elasticContext;

    @Before
    public void init() throws Exception {
        fixture = new ElasticSearchSink();
        fixture.setName("ElasticSearchSink-" + UUID.randomUUID().toString());
        elasticContext = new Context();
        elasticContext.put("es.client.hosts", "10.12.1.23:9300");
        elasticContext.put("es.index.default.name", "reqtest");

    }

    @Test
    public void shouldIndexJsonEvent() throws EventDeliveryException, InterruptedException {

        fixture.configure(elasticContext);
        Channel channel = bindAndStartChannel(fixture);

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody("{\"name\":\"rex\", \"age\": 23}".getBytes());
        channel.put(event);
        tx.commit();
        tx.close();


        fixture.process();
        fixture.stop();


        Thread.sleep(1000L);

    }

    @Test
    public void testDayIndexNameBuilder() throws EventDeliveryException, InterruptedException {

        elasticContext.put("es.index.builder", "com.ai.renzq.flume.sink.es.index_builder.DayStatiIndexBuilder");
        elasticContext.put("es.index.default.name", "staffa");

        fixture.configure(elasticContext);
        Channel channel = bindAndStartChannel(fixture);

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder
                .withBody("{\"name\":\"rex\", \"age\": 23, \"timestamp\": \"2017-01-01 00:00:00\"}".getBytes());
        event.getHeaders().put("id", "1");
        channel.put(event);
        tx.commit();
        tx.close();


        fixture.process();
        fixture.stop();


        Thread.sleep(1000L);

    }

    @Test
    public void testIndexCvsEvent() throws EventDeliveryException, InterruptedException {
        Context csv_context = new Context();
        csv_context.put("es.client.hosts", "10.12.1.23:9300");
        csv_context.put("es.index.default.name", "reqtest");
        csv_context.put("es.serializer", "com.ai.renzq.flume.sink.es.serializer.CSVSerializer");
        csv_context.put("es.serializer.csv.fields", "create_date:string,id:string,name:string,rname:string");
        csv_context.put("es.index.id", "id");

        String body = "\"'2018-04-28 20:46:39'\",\"55785\",\"ksliuyin\",\"??\"";

        fixture.configure(csv_context);
        Channel channel = bindAndStartChannel(fixture);

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody(body.getBytes());
        channel.put(event);
        tx.commit();
        tx.close();


        fixture.process();
        fixture.stop();

        Thread.sleep(1000L);

    }
}
