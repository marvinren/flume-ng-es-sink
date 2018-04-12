package com.ai.renzq.flume.sink.es.serializer;


import com.google.common.base.Throwables;
import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.ai.renzq.flume.sink.es.ElasticSearchSinkConstants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @ProjectName: flumesinkes
 * @Package: com.ai.renzq.flume.sink.es
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/11 11:24
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/11 11:24
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class CSVSerializer implements ISerializer {

    private static final Logger logger = LoggerFactory.getLogger(CSVSerializer.class);

    private final List<String> names = new ArrayList<String>();

    private final List<String> types = new ArrayList<String>();

    private String delimiter;

    /**
     *
     * Converts the csv data to the json format
     */
    @Override
    public XContentBuilder serialize(Event event) {
        XContentBuilder xContentBuilder = null;
        String body = new String(event.getBody(), Charsets.UTF_8);
        try {
            if (!names.isEmpty() && !types.isEmpty()) {
                xContentBuilder = jsonBuilder().startObject();
                List<String> values = Arrays.asList(body.split(delimiter));
                for (int i = 0; i < names.size(); i++) {
                    this.addField(xContentBuilder, names.get(i), values.get(i), types.get(i));
                }
                xContentBuilder.endObject();
            } else {
                logger.error("Fields for csv files are not configured, " +
                        "please configured the property " + ES_SERIALIZER_CSV_FIELD);
            }
        } catch (Exception e) {
            logger.error("Error in converting the body to the json format " + e.getMessage(), e);
        }
        return xContentBuilder;
    }

    /**
     *
     * Returns name and value based on the index
     *
     */
    private String getValue(String fieldType, Integer index) {
        String value = "";
        if (fieldType.length() > index) {
            value = fieldType.split(COLONS)[index];
        }
        return value;
    }

    /**
     *
     * Configure the field and its type with the custom delimiter
     */
    @Override
    public void configure(Context context) {
        String fields = context.getString(ES_SERIALIZER_CSV_FIELD);
        if(fields == null) {
            Throwables.propagate(new Exception("Fields for csv files are not configured," +
                    " please configured the property " + ES_SERIALIZER_CSV_FIELD));
        }
        try {
            delimiter = context.getString(ES_CSV_DELIMITER, DEFAULT_ES_CSV_DELIMITER);
            String[] fieldTypes = fields.split(delimiter);
            for (String fieldType : fieldTypes) {
                names.add(getValue(fieldType, 0));
                types.add(getValue(fieldType, 1));
            }
        } catch(Exception e) {
            Throwables.propagate(e);
        }
    }
}
