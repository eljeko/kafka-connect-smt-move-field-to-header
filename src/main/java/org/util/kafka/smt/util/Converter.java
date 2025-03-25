package org.util.kafka.smt.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Converter {


  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Infer Schema from JSON
  public static Schema inferSchema(JsonNode node) {
    if (node.isObject()) {
      SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        JsonNode fieldValue = field.getValue();

        // Recursively determine the field type
        Schema fieldSchema = inferSchema(fieldValue);
        structSchemaBuilder.field(fieldName, fieldSchema);
      }

      return structSchemaBuilder.build();
    } else if (node.isArray()) {
      return SchemaBuilder.array(Schema.STRING_SCHEMA).build();  // Assuming array of strings for simplicity
    } else if (node.isTextual()) {
      return Schema.STRING_SCHEMA;
    } else if (node.isInt() || node.isLong()) {
      return Schema.INT32_SCHEMA;
    } else if (node.isBoolean()) {
      return Schema.BOOLEAN_SCHEMA;
    }

    // Add more type checks as needed (e.g., for decimals, floats, etc.)
    return Schema.STRING_SCHEMA;  // Default to STRING if unknown
  }
  public static Struct covertJsonToStruc(String json, Schema schema) {
    if (json == null || schema == null) return null;

    try {
      // Deserialize the JSON string into a Map
      Map<String, Object> jsonMap = objectMapper.readValue(json, Map.class);

      // Create a new Struct based on the schema
      Struct struct = new Struct(schema);

      // Iterate over the schema fields and populate the struct
      for (Field field : schema.fields()) {
        Object fieldValue = jsonMap.get(field.name());

        if (fieldValue instanceof Map) {
          // If the field value is a nested Map, recursively convert it to a Struct
          Schema fieldSchema = field.schema();
          if (fieldSchema != null && fieldSchema.type() == Schema.Type.STRUCT) {
            struct.put(field, covertJsonToStruc(objectMapper.writeValueAsString(fieldValue), fieldSchema));
          }
        } else {
          // For regular fields, just set the value
          struct.put(field, fieldValue);
        }
      }

      return struct;
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize JSON to Struct", e);
    }
  }

  public static String convertStructToJson(Struct struct) {
    if (struct == null) return null;

    Map<String, Object> jsonMap = new HashMap<>();
    Schema schema = struct.schema();

    // Iterate over fields in the schema
    for (Field field : schema.fields()) {
      Object fieldValue = struct.get(field);

      // Check if the field is a Struct (nested struct)
      if (fieldValue instanceof Struct) {
        // Recursively serialize the nested struct to JSON
        jsonMap.put(field.name(), innerConvertStructToJson((Struct) fieldValue));
      } else {
        // Otherwise, just add the field value
        jsonMap.put(field.name(), fieldValue);
      }
    }

    try {
      return objectMapper.writeValueAsString(jsonMap);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize Struct to JSON", e);
    }
  }

  private static Map<String, Object> innerConvertStructToJson(Struct struct) {
    if (struct == null) return null;

    Map<String, Object> nestedJsonMap = new HashMap<>();
    Schema schema = struct.schema();

    for (Field field : schema.fields()) {
      Object fieldValue = struct.get(field);

      // If the field is a nested Struct, recursively call convertStructToJson
      if (fieldValue instanceof Struct) {
        nestedJsonMap.put(field.name(), innerConvertStructToJson((Struct) fieldValue));
      } else {
        nestedJsonMap.put(field.name(), fieldValue);
      }
    }

    return nestedJsonMap; // Return as a map instead of a string
  }
  /*
  public static String convertStructToJson(Struct struct) throws JsonProcessingException {
    Map<String, Object> jsonMap = new HashMap<>();

    // Get fields from the schema
    List<Field> fields = struct.schema().fields();
    for (Field field : fields) {
      String fieldName = field.name();
      Schema.Type fieldType = field.schema().type();
      switch (fieldType) {
        case STRING:
          jsonMap.put(fieldName, struct.getString(fieldName));
          break;
        case INT32:
          jsonMap.put(fieldName, struct.getInt32(fieldName));
          break;
        // Add more case statements for other data types here as needed
        case BOOLEAN:
          jsonMap.put(fieldName, struct.getBoolean(fieldName));
          break;
        case ARRAY:
          jsonMap.put(fieldName, struct.getArray(fieldName));
          break;
        case STRUCT:
          jsonMap.put(fieldName, convertStructToJson(struct.getStruct(fieldName)));
          // Recursive call for nested structs
          break;
        default:
          jsonMap.put(fieldName, struct.get(fieldName)); // Default case to catch other types
      }
    }

    // Convert map to JSON string
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(jsonMap);
  }

  */
}
