package ru.isands.newconverter.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.opencsv.CSVWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import ru.isands.newconverter.enums.Format;
import ru.isands.newconverter.exception.ConversionException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author batal
 * @Date 06.02.2026
 */
@Component
public class WriteUtil {
    @Value("${app.temp-dir}")
    private String tempDir;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final XmlMapper xmlMapper = new XmlMapper();

    public Resource writeToParquet(List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) {
            throw new ConversionException("Cannot write empty data to Parquet");
        }
        
        File tempFile = null;
        try {
            tempFile = File.createTempFile("parquet_", Format.PARQUET.getSuffix(), new File(tempDir));
            List<Map<String, Object>> typedData = convertToTypedData(data);
            Schema schema = inferSchema(typedData);
            
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(new org.apache.hadoop.fs.Path(tempFile.getAbsolutePath()))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
                for (Map<String, Object> record : typedData) {
                    GenericRecord avroRecord = new GenericData.Record(schema);
                    for (Map.Entry<String, Object> entry : record.entrySet()) {
                        avroRecord.put(entry.getKey(), convertToAvroType(entry.getValue(),
                                schema.getField(entry.getKey()).schema()));
                    }
                    writer.write(avroRecord);
                }
            }
            return new UrlResource(tempFile.toURI());
        } catch (IOException e) {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
            throw new ConversionException("Failed to write Parquet file: " + e.getMessage(), e);
        }
    }
    public Resource writeToCsv(List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) {
            throw new ConversionException("Cannot write empty data to CSV");
        }
        
        File tempFile = null;
        try {
            tempFile = File.createTempFile("csv_", Format.CSV.getSuffix(), new File(tempDir));
            try (CSVWriter writer = new CSVWriter(new FileWriter(tempFile, StandardCharsets.UTF_8))) {
                String[] headers = data.get(0).keySet().toArray(new String[0]);
                writer.writeNext(headers);
                
                for (Map<String, Object> record : data) {
                    String[] row = new String[headers.length];
                    for (int i = 0; i < headers.length; i++) {
                        Object value = record.get(headers[i]);
                        row[i] = value != null ? value.toString() : "";
                    }
                    writer.writeNext(row);
                }
            }
            return new UrlResource(tempFile.toURI());
        } catch (IOException e) {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
            throw new ConversionException("Failed to write CSV file: " + e.getMessage(), e);
        }
    }
    public Resource writeToJson(List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) {
            throw new ConversionException("Cannot write empty data to JSON");
        }
        
        File tempFile = null;
        try {
            tempFile = File.createTempFile("json_", Format.JSON.getSuffix(), new File(tempDir));
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            jsonMapper.writeValue(tempFile, data);
            return new UrlResource(tempFile.toURI());
        } catch (IOException e) {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
            throw new ConversionException("Failed to write JSON file: " + e.getMessage(), e);
        }
    }
    public Resource writeToXml(List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) {
            throw new ConversionException("Cannot write empty data to XML");
        }
        
        File tempFile = null;
        try {
            tempFile = File.createTempFile("xml_", Format.XML.getSuffix(), new File(tempDir));
            xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
            Map<String, Object> wrapper = new HashMap<>();
            wrapper.put("records", data);
            xmlMapper.writeValue(tempFile, wrapper);
            return new UrlResource(tempFile.toURI());
        } catch (IOException e) {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
            throw new ConversionException("Failed to write XML file: " + e.getMessage(), e);
        }
    }
    private List<Map<String, Object>> convertToTypedData(List<Map<String, Object>> data) {
        if (data.isEmpty()) return data;
        Map<String, Class<?>> columnTypes = new HashMap<>();
        Set<String> allKeys = new LinkedHashSet<>();
        for (Map<String, Object> record : data) {
            allKeys.addAll(record.keySet());
        }
        for (String key : allKeys) {
            columnTypes.put(key, determineColumnType(data, key));
        }
        List<Map<String, Object>> typedData = new ArrayList<>();
        for (Map<String, Object> record : data) {
            Map<String, Object> typedRecord = new LinkedHashMap<>();
            for (String key : allKeys) {
                Object value = record.get(key);
                Class<?> targetType = columnTypes.get(key);
                typedRecord.put(key, convertToType(value, targetType));
            }
            typedData.add(typedRecord);
        }
        return typedData;
    }

    private Class<?> determineColumnType(List<Map<String, Object>> data, String column) {
        boolean allNumbers = true;
        boolean hasDecimal = false;
        
        for (Map<String, Object> record : data) {
            Object value = record.get(column);
            if (value != null && value instanceof String) {
                String strValue = value.toString().trim();
                if (strValue.isEmpty()) {
                    continue;
                }
                
                // Check for number
                try {
                    if (strValue.contains(".") || strValue.contains("e") || strValue.contains("E")) {
                        Double.parseDouble(strValue);
                        hasDecimal = true;
                    } else {
                        Long.parseLong(strValue);
                    }
                } catch (NumberFormatException e) {
                    allNumbers = false;
                }
            } else if (value != null) {
                // Already typed value
                if (!(value instanceof Number)) {
                    allNumbers = false;
                } else if (value instanceof Double || value instanceof Float) {
                    hasDecimal = true;
                }
            }
        }
        
        if (allNumbers) {
            if (hasDecimal) {
                return Double.class;
            } else {
                boolean fitsInInteger = true;
                for (Map<String, Object> record : data) {
                    Object value = record.get(column);
                    if (value != null) {
                        String strValue = value.toString().trim();
                        if (!strValue.isEmpty()) {
                            try {
                                long longValue = Long.parseLong(strValue);
                                if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                                    fitsInInteger = false;
                                    break;
                                }
                            } catch (NumberFormatException e) {
                                // Skip
                            }
                        }
                    }
                }
                return fitsInInteger ? Integer.class : Long.class;
            }
        }
        
        // Check for boolean only if not all numbers
        boolean allBooleans = true;
        for (Map<String, Object> record : data) {
            Object value = record.get(column);
            if (value != null) {
                String strValue = value.toString().toLowerCase().trim();
                if (!strValue.isEmpty() &&
                        !strValue.equals("true") &&
                        !strValue.equals("false") &&
                        !strValue.equals("1") &&
                        !strValue.equals("0")) {
                    allBooleans = false;
                    break;
                }
            }
        }
        if (allBooleans) return Boolean.class;
        
        return String.class;
    }

    private Object convertToType(Object value, Class<?> targetType) {
        if (value == null || value.toString().isEmpty()) return null;
        String strValue = value.toString();
        try {
            return switch (targetType.getSimpleName()) {
                case "Integer" -> Integer.parseInt(strValue);
                case "Long" -> Long.parseLong(strValue);
                case "Double" -> Double.parseDouble(strValue);
                case "Boolean" -> parseBoolean(strValue);
                default -> strValue;
            };
        } catch (NumberFormatException e) {
            return strValue;
        }
    }
    private Object convertToAvroType(Object value, Schema schema) {
        if (value == null) return null;
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema type : schema.getTypes()) {
                if (type.getType() != Schema.Type.NULL) {
                    return convertToAvroType(value, type);
                }
            }
            return null;
        }
        switch (schema.getType()) {
            case INT:
                if (value instanceof Number) return ((Number) value).intValue();
                break;
            case LONG:
                if (value instanceof Number) return ((Number) value).longValue();
                break;
            case FLOAT:
                if (value instanceof Number) return ((Number) value).floatValue();
                break;
            case DOUBLE:
                if (value instanceof Number) return ((Number) value).doubleValue();
                break;
            case BOOLEAN:
                if (value instanceof Boolean) return value;
                break;
        }
        return value.toString();
    }
    private Schema inferSchema(List<Map<String, Object>> typedData) {
        if (typedData.isEmpty()) {
            throw new ConversionException("Cannot infer schema from empty data");
        }
        Set<String> allKeys = new LinkedHashSet<>();
        for (Map<String, Object> record : typedData) {
            allKeys.addAll(record.keySet());
        }
        List<Schema.Field> fields = new ArrayList<>();
        for (String key : allKeys) {
            Schema fieldSchema = inferFieldSchema(typedData, key);
            Schema nullableSchema = Schema.createUnion(
                    Arrays.asList(Schema.create(Schema.Type.NULL), fieldSchema)
            );
            fields.add(new Schema.Field(key, nullableSchema, null, null));
        }
        return Schema.createRecord("Record", null, null, false, fields);
    }
    
    private Schema inferFieldSchema(List<Map<String, Object>> typedData, String fieldName) {
        Set<Class<?>> types = new HashSet<>();
        for (Map<String, Object> record : typedData) {
            Object value = record.get(fieldName);
            if (value != null) {
                types.add(value.getClass());
            }
        }
        if (types.isEmpty()) {
            return Schema.create(Schema.Type.STRING);
        }
        // Check if any type is a Map or List implementation
        for (Class<?> type : types) {
            if (Map.class.isAssignableFrom(type) || List.class.isAssignableFrom(type)) {
                return Schema.create(Schema.Type.STRING);
            }
        }
        Schema.Type priorityType = null;
        for (Class<?> type : types) {
            priorityType = switch (type.getSimpleName()) {
                case "Double" -> Schema.Type.DOUBLE;
                case "Float" -> Schema.Type.FLOAT;
                case "Long" -> Schema.Type.LONG;
                case "Integer" -> Schema.Type.INT;
                case "Boolean" -> Schema.Type.BOOLEAN;
                default -> null;
            };
            if (priorityType != null) {
                break;
            }
        }
        return priorityType != null ? Schema.create(priorityType) : Schema.create(Schema.Type.STRING);
    }
    
    private boolean parseBoolean(String value) {
        String lowerValue = value.toLowerCase().trim();
        return lowerValue.equals("true") || lowerValue.equals("1");
    }
}
