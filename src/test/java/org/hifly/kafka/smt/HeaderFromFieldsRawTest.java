package org.hifly.kafka.smt;

import java.util.HashMap;
import java.util.Map;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.util.kafka.smt.HeaderFromFields;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class HeaderFromFieldsRawTest {

    private final HeaderFromFields<SourceRecord> xform = new HeaderFromFields();

    @AfterEach
    public void teardown() {
    }

    @Test
    public void schemaless() {

        Map<String, String> m = new HashMap<>();

        m.put("headersnames", "header1,header2");
        m.put("jsonpaths", "$.TOHEADER1,$.TOHEADER2");
        xform.configure(m);

        String header_1_value = "20400";
        String header_2_value = "200000002";

        String str = "{\n" +
                "  \"FIELD1\": \"01\",\n" +
                "  \"FIELD2\": \"02\",\n" +
                "  \"TOHEADER1\": \"" + header_1_value + "\",\n" +
                "  \"FIELD3\": \"001\",\n" +
                "  \"FIELD4\": \"0006084655017\",\n" +
                "  \"TOHEADER2\": \"" + header_2_value + "\",\n" +
                "  \"FIELD6\": 9000018}";

        final SourceRecord record = new  SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, str);
        final SourceRecord transformedRecord = xform.apply(record);


        assertEquals(2, transformedRecord.headers().size());

        assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

        assertEquals(header_2_value, transformedRecord.headers().allWithName("header2").next().value());

        JSONObject jsonOutput = new JSONObject(transformedRecord.value().toString());
        System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
    }


    @Test
    public void schemaless_one() {

        Map<String, String> m = new HashMap<>();

        m.put("headersnames", "header1");
        m.put("jsonpaths", "$.TOHEADER1");
        xform.configure(m);

        String header_1_value = "20400";
        String header_2_value = "200000002";

        String str = "{\n" +
                "  \"FIELD1\": \"01\",\n" +
                "  \"FIELD2\": \"02\",\n" +
                "  \"TOHEADER1\": \"" + header_1_value + "\",\n" +
                "  \"FIELD3\": \"001\",\n" +
                "  \"FIELD4\": \"0006084655017\",\n" +
                "  \"TOHEADER2\": \"" + header_2_value + "\",\n" +
                "  \"FIELD6\": 9000018}";

        final SourceRecord record = new  SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, str);
        final SourceRecord transformedRecord = xform.apply(record);


        assertEquals(1, transformedRecord.headers().size());

        assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

        JSONObject jsonOutput = new JSONObject(transformedRecord.value().toString());
        System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
    }


    @Test
    public void schemaless_nested() {

        Map<String, String> m = new HashMap<>();

        m.put("headersnames", "header1,header2,header3");
        m.put("jsonpaths", "$.TOHEADER1,$.TOHEADER2,$.FIELD_OUT.TOHEADEROUT1");
        xform.configure(m);

        String header_1_value = "20400";
        String header_2_value = "200000002";
        String header_3_value = "9999";

        String str = "{\n" +
                "  \"FIELD2\": \"02\",\n" +
                "  \"FIELD3\": \"001\",\n" +
                "  \"FIELD4\": \"0006084655017\",\n" +
                "  \"FIELD_OUT\": {\n" +
                "    \"TOHEADEROUT1\": \"9999\"\n" +
                "  },\n" +
                "  \"FIELD6\": 9000018,\n" +
                "  \"TOHEADER1\": \"20400\",\n" +
                "  \"TOHEADER2\": \"200000002\",\n" +
                "  \"FIELD1\": \"01\"\n" +
                "}";

        final SourceRecord record = new  SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, str);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(3, transformedRecord.headers().size());

        assertEquals(header_1_value, transformedRecord.headers().allWithName("header1").next().value());

        assertEquals(header_2_value, transformedRecord.headers().allWithName("header2").next().value());

        assertEquals(header_3_value, transformedRecord.headers().allWithName("header3").next().value());

        JSONObject jsonOutput = new JSONObject(transformedRecord.value().toString());
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

        //   xform.configure(m);
        //Map<String, String> fields = Collections.singletonMap("fields", "FIELD1,FIELD2,FIELD3");
        //fields.put("jsonpath", "$.TOHEADER");
        xform.configure(m);
        //xform.configure(Collections.singletonMap("fields", "FIELD1,FIELD2,FIELD3"));

        String header_1_value = "20400";
        String header_2_value = "200000002";

        String str = "{\n" +
                "  \"FIELD1\": \"01\",\n" +
                "  \"FIELD2\": \"02\",\n" +
                "  \"TOHEADER1\": \"" + header_1_value + "\",\n" +
                "  \"FIELD3\": \"001\",\n" +
                "  \"FIELD4\": \"0006084655017\",\n" +
                "  \"TOHEADER2\": \"" + header_2_value + "\",\n" +
                "  \"FIELD6\": 9000018}";

        final SourceRecord record = new  SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, str);
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


        JSONObject jsonOutput = new JSONObject(transformedRecord.value().toString());
        System.out.println("Transformed record value with removed fields:\n" + jsonOutput.toString(4));
    }

    @Test
    public void schemaless_remove_unexisting_field() {

        Map<String, String> m = new HashMap<>();

        m.put("headersnames", "header1,header2");
        m.put("jsonpaths", "$.TOHEADER99,$.TOHEADER88");
        xform.configure(m);

        String header_1_value = "20400";
        String header_2_value = "200000002";

        String str = "{\n" +
                "  \"FIELD1\": \"01\",\n" +
                "  \"FIELD2\": \"02\",\n" +
                "  \"TOHEADER1\": \"" + header_1_value + "\",\n" +
                "  \"FIELD3\": \"001\",\n" +
                "  \"FIELD4\": \"0006084655017\",\n" +
                "  \"TOHEADER2\": \"" + header_2_value + "\",\n" +
                "  \"FIELD6\": 9000018}";

        final SourceRecord record = new  SourceRecord(new HashMap<>(), new HashMap<>(), null, null, null, str);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(0, transformedRecord.headers().size());


        JSONObject jsonOutput = new JSONObject(transformedRecord.value().toString());
        System.out.println("Transformed record value:\n" + jsonOutput.toString(4));
    }

}
