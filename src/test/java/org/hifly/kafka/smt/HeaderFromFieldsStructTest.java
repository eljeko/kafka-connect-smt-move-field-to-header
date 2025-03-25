package org.hifly.kafka.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.util.kafka.smt.util.Converter;
import org.util.kafka.smt.HeaderFromFields;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class HeaderFromFieldsStructTest {

  private final HeaderFromFields<SourceRecord> xform = new HeaderFromFields();

  @AfterEach
  public void teardown() {
  }

  @Test
  public void schemaless() throws JsonProcessingException {

    Map<String, String> m = new HashMap<>();

    m.put("headersnames", "header1,header2");
    m.put("jsonpaths", "$.TOHEADER1,$.TOHEADER2");
    m.put("inputType", "struct");
    xform.configure(m);

    String header_1_value = "20400";
    String header_2_value = "200000002";


    final Schema inputSchema = SchemaBuilder.struct()
            .field("FIELD1", Schema.STRING_SCHEMA)
            .field("TOHEADER1", Schema.STRING_SCHEMA)
            .field("TOHEADER2", Schema.STRING_SCHEMA)
            .field("FIELD2", Schema.STRING_SCHEMA)
            .field("FIELD3", Schema.STRING_SCHEMA)
            .field("FIELD4", Schema.STRING_SCHEMA)
            .field("FIELD6", Schema.INT32_SCHEMA)
            .build();


    final Struct inputStruct = new Struct(inputSchema)
            .put("FIELD1", "01")
            .put("TOHEADER1", header_1_value)
            .put("FIELD2", "02")
            .put("FIELD3", "03")
            .put("TOHEADER2", header_2_value)
            .put("FIELD4", "0006084655017")
            .put("FIELD6", 9000018);

    final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, inputStruct);
    final SourceRecord transformedRecord = xform.apply(record);


    assertEquals(2, transformedRecord.headers().size());

    assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

    assertEquals(header_2_value, transformedRecord.headers().allWithName("header2").next().value());

    JSONObject jsonOutput = new JSONObject(Converter.convertStructToJson((Struct) transformedRecord.value()));
    System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
  }


  @Test
  public void schemaless_one() throws JsonProcessingException {

    Map<String, String> m = new HashMap<>();

    m.put("headersnames", "header1");
    m.put("jsonpaths", "$.TOHEADER1");
    m.put("inputType", "struct");
    xform.configure(m);

    String header_1_value = "20400";

    final Schema inputSchema = SchemaBuilder.struct()
            .field("FIELD1", Schema.STRING_SCHEMA)
            .field("TOHEADER1", Schema.STRING_SCHEMA)
            .field("FIELD2", Schema.STRING_SCHEMA)
            .build();


    final Struct inputStruct = new Struct(inputSchema)
            .put("FIELD1", "01")
            .put("TOHEADER1", header_1_value)
            .put("FIELD2", "02");


    final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, inputStruct);
    final SourceRecord transformedRecord = xform.apply(record);


    assertEquals(1, transformedRecord.headers().size());

    assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

    JSONObject jsonOutput = new JSONObject(Converter.convertStructToJson((Struct) transformedRecord.value()));
    System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
  }


  @Test
  public void schemaless_nested() {

    Map<String, String> m = new HashMap<>();

    m.put("headersnames", "header1,header2,header3");
    m.put("jsonpaths", "$.TOHEADER1,$.TOHEADER2,$.FIELD_OUT.TOHEADEROUT1");
    m.put("inputType", "struct");
    xform.configure(m);

    String header_1_value = "20400";
    String header_2_value = "200000002";
    String header_3_value = "9999";


    final Schema innerSchema = SchemaBuilder.struct()
            .name("FIELD_OUT")
            .field("TOHEADEROUT1", Schema.STRING_SCHEMA);

    final Schema inputSchema = SchemaBuilder.struct()
            .field("FIELD1", Schema.STRING_SCHEMA)
            .field("TOHEADER1", Schema.STRING_SCHEMA)
            .field("TOHEADER2", Schema.STRING_SCHEMA)
            .field("FIELD2", Schema.STRING_SCHEMA)
            .field("FIELD3", Schema.STRING_SCHEMA)
            .field("FIELD_OUT", innerSchema)
            .field("FIELD4", Schema.STRING_SCHEMA)
            .field("FIELD6", Schema.INT32_SCHEMA)
            .build();


    final Struct innerStruct = new Struct(innerSchema);

    innerStruct.put("TOHEADEROUT1", header_3_value);
    final Struct inputStruct = new Struct(inputSchema)
            .put("FIELD1", "01")
            .put("TOHEADER1", header_1_value)
            .put("TOHEADER2", header_2_value)
            .put("FIELD2", "02")
            .put("FIELD3", "03")
            .put("FIELD_OUT", innerStruct)
            .put("FIELD4", "0006084655017")
            .put("FIELD6", 9000018);

    final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, inputStruct);
    final SourceRecord transformedRecord = xform.apply(record);


    assertEquals(3, transformedRecord.headers().size());

    assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

    assertEquals(header_2_value, transformedRecord.headers().allWithName("header2").next().value());

    assertEquals(header_3_value, transformedRecord.headers().allWithName("header3").next().value());

    JSONObject jsonOutput = new JSONObject(Converter.convertStructToJson((Struct) transformedRecord.value()));
    System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
  }

  @Test
  public void schemaless_remove_fields() {

    Map<String, String> m = new HashMap<>();


    m.put("headersnames", "header1,header2");

    String json_path_01 = "$.TOHEADER1";
    String json_path_02 = "$.TOHEADER2";

    m.put("jsonpaths", json_path_01 + "," + json_path_02);
    m.put("mode", "move");
    m.put("inputType", "struct");

    xform.configure(m);

    String header_1_value = "20400";
    String header_2_value = "200000002";


    final Schema inputSchema = SchemaBuilder.struct()
            .field("FIELD1", Schema.STRING_SCHEMA)
            .field("TOHEADER1", Schema.STRING_SCHEMA)
            .field("TOHEADER2", Schema.STRING_SCHEMA)
            .field("FIELD2", Schema.STRING_SCHEMA)
            .field("FIELD3", Schema.STRING_SCHEMA)
            .field("FIELD4", Schema.STRING_SCHEMA)
            .field("FIELD6", Schema.INT32_SCHEMA)
            .build();


    final Struct inputStruct = new Struct(inputSchema)
            .put("FIELD1", "01")
            .put("TOHEADER1", header_1_value)
            .put("FIELD2", "02")
            .put("FIELD3", "03")
            .put("TOHEADER2", header_2_value)
            .put("FIELD4", "0006084655017")
            .put("FIELD6", 9000018);

    final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, inputStruct);
    final SourceRecord transformedRecord = xform.apply(record);



    assertEquals(2, transformedRecord.headers().size());

    assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

    assertEquals(header_2_value, transformedRecord.headers().allWithName("header2").next().value());

    ReadContext ctx = JsonPath.parse(transformedRecord.value().toString());


    try {
      Object value = ctx.read(json_path_01);
      fail("Expected JSONPath " + json_path_01 + " to be removed, but found value: " + value);
    } catch (Exception e) {
      //This is expected!!!
    }

    try {
      Object value = ctx.read(json_path_02);
      fail("Expected JSONPath " + json_path_02 + " to be removed, but found value: " + value);
    } catch (Exception e) {
      //This is expected!!!
    }


    JSONObject jsonOutput = new JSONObject(Converter.convertStructToJson((Struct) transformedRecord.value()));
    System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
  }

  @Test
  public void schemaless_remove_unexisting_field() {

    Map<String, String> m = new HashMap<>();

    m.put("headersnames", "header1,header2");
    m.put("jsonpaths", "$.TOHEADER99,$.TOHEADER88");
    m.put("inputType", "struct");
    xform.configure(m);
    String header_1_value = "20400";
    String header_2_value = "200000002";


    final Schema inputSchema = SchemaBuilder.struct()
            .field("FIELD1", Schema.STRING_SCHEMA)
            .field("TOHEADER1", Schema.STRING_SCHEMA)
            .field("TOHEADER2", Schema.STRING_SCHEMA)
            .field("FIELD2", Schema.STRING_SCHEMA)
            .field("FIELD3", Schema.STRING_SCHEMA)
            .field("FIELD4", Schema.STRING_SCHEMA)
            .field("FIELD6", Schema.INT32_SCHEMA)
            .build();


    final Struct inputStruct = new Struct(inputSchema)
            .put("FIELD1", "01")
            .put("TOHEADER1", header_1_value)
            .put("FIELD2", "02")
            .put("FIELD3", "03")
            .put("TOHEADER2", header_2_value)
            .put("FIELD4", "0006084655017")
            .put("FIELD6", 9000018);

    final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, inputStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(0, transformedRecord.headers().size());


    JSONObject jsonOutput = new JSONObject(Converter.convertStructToJson((Struct) transformedRecord.value()));
    System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
  }

}
