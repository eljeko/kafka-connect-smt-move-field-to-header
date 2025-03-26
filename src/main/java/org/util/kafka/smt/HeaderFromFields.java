/**
 * Copyright © 2025
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.util.kafka.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.util.kafka.smt.util.Converter;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;


public class HeaderFromFields<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String JSON_PATHS = "jsonpaths";
  public static final String HEADERS_NAMES = "headersnames";
  private static final String MODE = "mode";
  private static final String INPUT_TYPE = "inputType";
  private static final String INPUT_TYPE_RAW = "raw";
  private static final String INPUT_TYPE_STRUCT = "struct";


  public static final ConfigDef CONFIG_DEF = new ConfigDef()
          .define(JSON_PATHS, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyString(), ConfigDef.Importance.HIGH, "The comma separated values of the json paths.")
          .define(HEADERS_NAMES, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyString(), ConfigDef.Importance.HIGH, "The comma separated values of the fields names.")
          .define(MODE, ConfigDef.Type.STRING, "copy", ConfigDef.Importance.LOW, "The property to define if the field is copied or moved.")
          .define(INPUT_TYPE, ConfigDef.Type.STRING, "raw", ConfigDef.Importance.LOW, "Define if the source is raw (serialized string representing a json) or a data Kadka Connect Data Struct.");

  private static final Logger LOGGER = LoggerFactory.getLogger(HeaderFromFields.class);

  private List<String> fields;
  // @formatter:off
  private Map<String, Schema> schemaMapping = new HashMap<>() {{
      put(String.class.getName(), OPTIONAL_STRING_SCHEMA);
      put(Integer.class.getName(), OPTIONAL_INT32_SCHEMA);
      put("org.json.JSONObject$Null", OPTIONAL_STRING_SCHEMA);
      }};
  // @formatter:on
  private List<String> jsonPaths = new ArrayList<>();
  private List<String> headersNames = new ArrayList<>();
  private String mode = "copy";
  private String inputType = "raw";


  @Override
  public void configure(Map<String, ?> configs) {
    jsonPaths = Arrays.stream(((String) configs.get(JSON_PATHS)).split(",")).toList();
    headersNames = Arrays.stream(((String) configs.get(HEADERS_NAMES)).split(",")).toList();
    if (configs.get(MODE) != null) {
      mode = configs.get(MODE).toString();
    }

    if (configs.get(INPUT_TYPE) != null) {
      inputType = configs.get(INPUT_TYPE).toString();
    }
  }

  @Override
  public R apply(R record) {
    try {
      return applySchemaless(record);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private String getStructAsString(R record) {
    return Converter.convertStructToJson((Struct) record.value());

  }

  private R applySchemaless(R record) throws JsonProcessingException {

    String value = "";

    if (inputType.equals(INPUT_TYPE_STRUCT)) {
      value = getStructAsString(record);
    } else {
      value = record.value().toString();
    }

    JSONObject jsonValueObject = new JSONObject(value);

    Schema keySchemaString = STRING_SCHEMA;

    StringBuilder keyValue = new StringBuilder();

    //Create schema for value
    SchemaBuilder schemaForValue = createSchemaForValue(jsonValueObject);

    ReadContext ctx = JsonPath.parse(value);

    for (int i = 0; i < jsonPaths.size(); i++) {

      String nextJsonPath = jsonPaths.get(i);
      String nextHeaderName = headersNames.get(i);

      try {
        String headerValue = ctx.read(nextJsonPath, String.class);
        Headers headers = record.headers();
        headers.add(nextHeaderName, headerValue, OPTIONAL_STRING_SCHEMA);

        if (mode.equals("move")) {
          try {
            // Extract the actual field name from JSONPath
            String fieldName = nextJsonPath.replace("$.", "");  // Convert "$.FIELD" -> "FIELD"
            jsonValueObject.remove(fieldName);
          } catch (Exception e) {
            LOGGER.warn("Failed to remove field {}: {}", nextJsonPath, e.getMessage());
          }
        }

      } catch (Exception e) {
        LOGGER.warn("Failed to extract TOHEADER field: {}", e.getMessage());
      }
    }

    if (mode.equals("move")) {
      if (inputType.equals(INPUT_TYPE_STRUCT)) {
        Schema schema = Converter.inferSchema(new ObjectMapper().readTree(jsonValueObject.toString()));
        Struct structValueObject = Converter.covertJsonToStruc(jsonValueObject.toString(), schema);
        return record.newRecord(record.topic(), record.kafkaPartition(), keySchemaString, keyValue.toString(), schemaForValue.schema(), structValueObject, record.timestamp());
      } else {
        return record.newRecord(record.topic(), record.kafkaPartition(), keySchemaString, keyValue.toString(), schemaForValue.schema(), jsonValueObject.toString(), record.timestamp());
      }

    } else {
      return record;
    }
  }

  private SchemaBuilder createSchemaForValue(JSONObject jsonObject) {

    //Create schema for value
    SchemaBuilder schemaStruct = SchemaBuilder.struct();
    for (Object keyObj : jsonObject.keySet()) {
      String key = (String) keyObj;
      //Inner json objects are treated as String
      if (jsonObject.get(key) instanceof JSONObject) {
        schemaStruct.field(key, STRING_SCHEMA).build();
      } else {
        schemaStruct.field(key, schemaMapping.get(jsonObject.get(key).getClass().getName())).build();
      }
    }
    return schemaStruct;
  }


  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}