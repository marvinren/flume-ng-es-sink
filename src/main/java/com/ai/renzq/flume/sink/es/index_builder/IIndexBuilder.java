package com.ai.renzq.flume.sink.es.index_builder;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.index_builder
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/11 16:36
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/11 16:36
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public interface IIndexBuilder extends Configurable {

    String getIndex(Event event);

    String getType(Event event);

    String getId(Event event);

}
