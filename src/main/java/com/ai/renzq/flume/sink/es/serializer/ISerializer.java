package com.ai.renzq.flume.sink.es.serializer;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.IOException;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es.serializer
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/11 13:51
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/11 13:51
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public interface ISerializer extends Configurable{

    static final Logger logger = LoggerFactory.getLogger(ISerializer.class);


    XContentBuilder serialize(Event event);


    public enum FieldTypeEnum {
        STRING("string"),
        INT("int"),
        FLOAT("float"),
        BOOLEAN("boolean");

        private String fieldType;

        FieldTypeEnum(String fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public String toString() {
            return fieldType;
        }
    }

    default public void addField(XContentBuilder xContentBuilder,String key, String value, String type) throws IOException {
        if (type != null) {
            FieldTypeEnum fieldTypeEnum = FieldTypeEnum.valueOf(type.toUpperCase());
            switch (fieldTypeEnum) {
                case STRING:
                    xContentBuilder.field(key, value);
                    break;
                case FLOAT:
                    xContentBuilder.field(key, Float.valueOf(value));
                    break;
                case INT:
                    xContentBuilder.field(key, Integer.parseInt(value));
                    break;
                case BOOLEAN:
                    xContentBuilder.field(key, Boolean.valueOf(value));
                    break;
                default:
                    logger.error("Type is incorrect, please check type: " + type);
            }
        }
    }
}
