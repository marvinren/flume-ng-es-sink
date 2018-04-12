package com.ai.renzq.flume.sink.es.serializer;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.serializer
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/11 16:17
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/11 16:17
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class JsonSerializer implements ISerializer {

    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);

    @Override
    public XContentBuilder serialize(Event event) {
        XContentBuilder builder = null;
        try {
            String event_body = new String(event.getBody(), Charsets.UTF_8);
            // append the @timestamp
            event_body = event_body.trim();
            if(event_body.charAt(event_body.length()-1) == '}'){
                event_body = event_body.substring(0, event_body.length() - 1)
                        + ", \"@timestamp\": " + System.currentTimeMillis() + "}";
            }
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            event_body);
            builder = jsonBuilder().copyCurrentStructure(parser);
            parser.close();
        } catch (Exception e) {
            logger.error("Error in Converting the body to json field " + e.getMessage(), e);
        }
        return builder;
    }

    @Override
    public void configure(Context context) {

    }
}
