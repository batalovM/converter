package ru.isands.newconverter.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import ru.isands.newconverter.enums.Format;
import ru.isands.newconverter.exception.ConversionException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author batal
 * @Date 06.02.2026
 */
@Component
public class ReadUtil {
    @Value("${app.temp-dir}")
    private String tempDir;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final XmlMapper xmlMapper = new XmlMapper();

    public List<Map<String, Object>> readParquet(MultipartFile file) {
        if (file.isEmpty()) {
            throw new ConversionException("Uploaded Parquet file is empty");
        }
        
        List<Map<String, Object>> result = new ArrayList<>();
        File tempFile = null;
        try {
            tempFile = File.createTempFile("parquet_input_", Format.PARQUET.getSuffix(), new File(tempDir));
            file.transferTo(tempFile);
            
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(new org.apache.hadoop.fs.Path(tempFile.getAbsolutePath()))
                    .disableCompatibility()
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (Schema.Field field : record.getSchema().getFields()) {
                        Object value = record.get(field.name());
                        map.put(field.name(), convertAvroValue(value));
                    }
                    result.add(map);
                }
            }
            return result;
        } catch (IOException e) {
            throw new ConversionException("Failed to read Parquet file: " + e.getMessage(), e);
        } finally {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
        }
    }
    public List<Map<String, Object>> readCsv(MultipartFile file) {
        if (file.isEmpty()) {
            throw new ConversionException("Uploaded CSV file is empty");
        }
        
        List<Map<String, Object>> result = new ArrayList<>();
        try (Reader reader = new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8);
             CSVReader csvReader = new CSVReader(reader)) {
            
            String[] headers = csvReader.readNext();
            if (headers == null || headers.length == 0) {
                return result;
            }
            
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String value = i < row.length ? row[i] : "";
                    map.put(headers[i], value);
                }
                result.add(map);
            }
            return result;
        } catch (IOException | CsvValidationException e) {
            throw new ConversionException("Failed to read CSV file: " + e.getMessage(), e);
        }
    }
    public List<Map<String, Object>> readJson(MultipartFile file) {
        if (file.isEmpty()) {
            throw new ConversionException("Uploaded JSON file is empty");
        }
        
        try {
            String content = new String(file.getBytes(), StandardCharsets.UTF_8).trim();
            if (content.isEmpty()) {
                throw new ConversionException("JSON file content is empty");
            }
            
            // Try to parse as array first
            if (content.startsWith("[")) {
                return jsonMapper.readValue(content, List.class);
            } else if (content.startsWith("{")) {
                // Parse as single object
                Map<String, Object> single = jsonMapper.readValue(content, Map.class);
                return Collections.singletonList(single);
            } else {
                throw new ConversionException("Invalid JSON format: must start with '[' or '{'");
            }
        } catch (IOException e) {
            throw new ConversionException("Failed to read JSON file: " + e.getMessage(), e);
        }
    }
    public List<Map<String, Object>> readXml(MultipartFile file) {
        if (file.isEmpty()) {
            throw new ConversionException("Uploaded XML file is empty");
        }
        
        try {
            String content = new String(file.getBytes(), StandardCharsets.UTF_8).trim();
            if (content.isEmpty()) {
                throw new ConversionException("XML file content is empty");
            }
            
            Map<String, Object> data = xmlMapper.readValue(content, Map.class);
            
            // Try to find a records wrapper element
            if (data.containsKey("records")) {
                Object records = data.get("records");
                if (records instanceof List) {
                    return (List<Map<String, Object>>) records;
                } else if (records instanceof Map) {
                    return Collections.singletonList((Map<String, Object>) records);
                }
            }
            
            // If no records wrapper, return the entire document as a single record
            return Collections.singletonList(data);
        } catch (IOException e) {
            throw new ConversionException("Failed to read XML file: " + e.getMessage(), e);
        }
    }
    private Object convertAvroValue(Object value) {
        if (value == null) return null;
        if (value instanceof org.apache.avro.util.Utf8) return value.toString();
        if (value instanceof java.nio.ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof GenericRecord record) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                map.put(field.name(), convertAvroValue(record.get(field.name())));
            }
            return map;
        }
        if (value instanceof GenericData.Array) {
            List<Object> list = new ArrayList<>();
            for (Object item : (GenericData.Array<?>) value) {
                list.add(convertAvroValue(item));
            }
            return list;
        }
        if (value instanceof Map<?, ?> original) {
            Map<Object, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : original.entrySet()) {
                result.put(
                        convertAvroValue(entry.getKey()),
                        convertAvroValue(entry.getValue())
                );
            }
            return result;
        }
        return value;
    }
}
