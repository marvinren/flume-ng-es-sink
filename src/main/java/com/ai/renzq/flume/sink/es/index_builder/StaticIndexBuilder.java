package com.ai.renzq.flume.sink.es.index_builder;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ai.renzq.flume.sink.es.ElasticSearchSinkConstants.*;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.index_builder
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/11 16:39
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/11 16:39
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class StaticIndexBuilder implements IIndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StaticIndexBuilder.class);

    private String index;

    private String type;

    @Override
    public String getIndex(Event event) {
        return this.index;
    }

    @Override
    public String getType(Event event) {
       return this.type;
    }

    @Override
    public String getId(Event event) {
        return null;
    }

    @Override
    public void configure(Context context) {
        this.index = context.getString(ES_INDEX_DEFAULT_NAME, DEFAULT_ES_INDEX);
        this.type = context.getString(ES_INDEX_DEFAULT_TYPE, DEFAULT_ES_TYPE);
        logger.info("Simple Index builder, name [{}] type [{}] ",
                new Object[]{this.index, this.type});

    }
}
