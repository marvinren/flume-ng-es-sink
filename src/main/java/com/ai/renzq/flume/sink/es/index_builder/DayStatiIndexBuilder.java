package com.ai.renzq.flume.sink.es.index_builder;

import com.google.common.base.Strings;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.ai.renzq.flume.sink.es.ElasticSearchSinkConstants.*;

/**
 * @ProjectName: flumengsinkes
 * @Package: com.ai.renzq.flume.sink.es.index_builder
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/12 16:24
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/12 16:24
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class DayStatiIndexBuilder  implements IIndexBuilder  {

    private static final Logger logger = LoggerFactory.getLogger(StaticIndexBuilder.class);

    private String index;

    private String type;

    @Override
    public String getIndex(Event event) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy_MM_dd");
        String day_index = this.index + "_" + sf.format(new Date());
        return day_index;
    }

    @Override
    public String getType(Event event) {
        return this.type;
    }

    @Override
    public String getId(Event event) {
        String id = event.getHeaders().get("id");
        if(Strings.isNullOrEmpty(id)) {
            return null;
        }else{
            return id;
        }
    }

    @Override
    public void configure(Context context) {
        this.index = context.getString(ES_INDEX_DEFAULT_NAME, DEFAULT_ES_INDEX);
        this.type = context.getString(ES_INDEX_DEFAULT_TYPE, DEFAULT_ES_TYPE);
        logger.info("Simple Index builder, name [{}] type [{}] ",
                new Object[]{this.index, this.type});

    }
}
