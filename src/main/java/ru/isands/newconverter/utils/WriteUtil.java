package ru.isands.newconverter.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.opencsv.CSVWriter;
import de.micromata.opengis.kml.v_2_2_0.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import ru.isands.newconverter.enums.Format;

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

    public Resource writeToKml(List<Map<String, Object>> data) throws IOException{
        File tempFile = File.createTempFile("kml_", Format.KML.getSuffix(), new File(tempDir));

        Kml kml = new Kml();
        Document document = new Document();
        document.setName("Converted Data");
        Map<String, Folder> folders = new HashMap<>();

        for(Map<String, Object> record: data){
            Placemark placemark = createPlacemarkFromRecord(record);

            String path = (String) record.get("folder_path");
            if(path != null && !path.isEmpty()){
                Folder folder = folders.computeIfAbsent(path, newPath ->{
                    Folder f = new Folder();
                    f.setName(getFolderNameFromPath(path));
                    return f;
                });
                folder.getFeature().add(placemark);
            }else {
                document.getFeature().add(placemark);
            }
        }
        for(Folder folder: folders.values()){
            document.getFeature().add(folder);
        }
        kml.setFeature(document);
        kml.marshal(tempFile);
        return new UrlResource(tempFile.toURI());
    }

    public Resource writeToParquet(List<Map<String, Object>> data) throws IOException {
        File tempFile = File.createTempFile("parquet_", Format.PARQUET.getSuffix(), new File(tempDir));
        List<Map<String, Object>> typedData = convertToTypedData(data);
        Schema schema = inferSchema(typedData);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new org.apache.hadoop.fs.Path(tempFile.getAbsolutePath()))
                .withSchema(schema)
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
    }
    public Resource writeToCsv(List<Map<String, Object>> data) throws IOException {
        File tempFile = File.createTempFile("file", Format.CSV.getSuffix());
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
    }
    public Resource writeToJson(List<Map<String, Object>> data) throws IOException{
        File tempFile = File.createTempFile("file", Format.JSON.getSuffix());
        jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
        jsonMapper.writeValue(tempFile, data);
        return new UrlResource(tempFile.toURI());
    }
    public Resource writeToXml(List<Map<String, Object>> data) throws IOException {
        File tempFile = File.createTempFile("file", Format.XML.getSuffix());
        xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("records", data);
        xmlMapper.writeValue(tempFile, wrapper);
        return new UrlResource(tempFile.toURI());
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
            if (value != null) {
                String strValue = value.toString();
                if (strValue.isEmpty()) {
                    continue;
                }
                try {
                    if (strValue.contains(".")) {
                        Double.parseDouble(strValue);
                        hasDecimal = true;
                    } else {
                        Long.parseLong(strValue);
                    }
                } catch (NumberFormatException e) {
                    allNumbers = false;
                    break;
                }
            }
        }
        if (allNumbers) return hasAllNumbers(hasDecimal, data, column);
        return hasAllBooleans(data, column);
    }
    private Class<?> hasAllNumbers(boolean hasDecimal, List<Map<String, Object>> data, String column){
        if (hasDecimal) {
            return Double.class;
        } else {
            boolean fitsInInteger = true;
            for (Map<String, Object> record : data) {
                Object value = record.get(column);
                if (value != null) {
                    String strValue = value.toString();
                    if (!strValue.isEmpty()) {
                        long longValue = Long.parseLong(strValue);
                        if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                            fitsInInteger = false;
                            break;
                        }
                    }
                }
            }
            return fitsInInteger ? Integer.class : Long.class;
        }
    }
    private Class<?> hasAllBooleans(List<Map<String, Object>> data, String column){
        boolean allBooleans = true;
        for (Map<String, Object> record : data) {
            Object value = record.get(column);
            if (value != null) {
                String strValue = value.toString().toLowerCase();
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
            throw new IllegalArgumentException("Cannot infer schema from empty data");
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
        for (Class<?> type : types) {
            if (type == Map.class || type == List.class) {
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
        return value.equalsIgnoreCase("true")
                || value.equalsIgnoreCase("false")
                || value.equals("1")
                || value.equals("0");
    }
    private String getFolderNameFromPath(String path){
        if(path == null || path.isEmpty()) return "Root";
        String[] parts = path.split("/");
        return parts[parts.length - 1];
    }
    private List<Coordinate> parseCoordinates(String coordStr){
        List<Coordinate> coordinates = new ArrayList<>();
        if(coordStr == null || coordStr.trim().isEmpty()){
            return coordinates;
        }
        String[] pairs = coordStr.trim().split("\\s+");
        for(String pair: pairs){
            String[] values = pair.split(",");
            if(values.length >= 2){
                double lon = Double.parseDouble(values[0]);
                double lat = Double.parseDouble(values[1]);
                double alt = values.length >= 3 ? Double.parseDouble(values[2]) : 0.0;
                coordinates.add(new Coordinate(lon, lat, alt));
            }
        }
        return coordinates;
    }
    private Placemark createPlacemarkFromRecord(Map<String, Object> record) {
        Placemark placemark = new Placemark();

        placemark.setName((String) record.getOrDefault("name", "Unnamed"));
        placemark.setDescription((String) record.getOrDefault("description", ""));

        String geometryType = (String) record.get("geometry_type");
        if ("Point".equals(geometryType) &&
                record.containsKey("longitude") &&
                record.containsKey("latitude")) {
            Point point = new Point();
            double lon = ((Number) record.get("longitude")).doubleValue();
            double lat = ((Number) record.get("latitude")).doubleValue();
            double alt = record.containsKey("altitude") ? ((Number) record.get("altitude")).doubleValue() : 0.0;
            point.addToCoordinates(lon + "," + lat + "," + alt);
            placemark.setGeometry(point);
        } else if ("LineString".equals(geometryType) && record.containsKey("coordinates")) {
            String coordStr = (String) record.get("coordinates");
            List<Coordinate> coordinates = parseCoordinates(coordStr);
            if (!coordinates.isEmpty()) {
                LineString line = new LineString();
                line.setCoordinates(coordinates);
                placemark.setGeometry(line);
            }
        } else if ("Polygon".equals(geometryType) && record.containsKey("outer_boundary")) {
            String coordStr = (String) record.get("outer_boundary");
            List<Coordinate> coords = parseCoordinates(coordStr);
            if (!coords.isEmpty()) {
                Polygon polygon = new Polygon();
                Boundary outer = new Boundary();
                LinearRing ring = new LinearRing();
                ring.setCoordinates(coords);
                outer.setLinearRing(ring);
                polygon.setOuterBoundaryIs(outer);
                placemark.setGeometry(polygon);
            }
        }
        return placemark;
    }
}
