package ru.isands.newconverter.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.micromata.opengis.kml.v_2_2_0.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import ru.isands.newconverter.enums.Format;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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

    public List<Map<String, Object>> readKml(MultipartFile file) throws IOException{
        List<Map<String, Object>> result = new ArrayList<>();
        String kmlContent = new String(file.getBytes(), StandardCharsets.UTF_8);
        Kml kml = Kml.unmarshal(kmlContent);
        if(kml.getFeature() instanceof Document document){
            extractFeatures(document.getFeature(), result, "");
        }else if(kml.getFeature() instanceof Placemark){
            Map<String, Object> record = convertPlacemarkToMap((Placemark) kml.getFeature(), "");
            result.add(record);
        }
        return result;
    }
    public List<Map<String, Object>> readParquet(MultipartFile file) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        File tempFile = File.createTempFile("parquet_input_", Format.PARQUET.getSuffix(), new File(tempDir));
        try {
            file.transferTo(tempFile);
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(new org.apache.hadoop.fs.Path(tempFile.getAbsolutePath()))
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
        } finally {
            tempFile.delete();
        }
        return result;
    }
    public List<Map<String, Object>> readCsv(MultipartFile file) throws IOException, CsvValidationException {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Reader reader = new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8);
             CSVReader csvReader = new CSVReader(reader)) {
            String[] headers = csvReader.readNext();
            if (headers == null) return result;
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String value = i < row.length ? row[i] : "";
                    map.put(headers[i], value);
                }
                result.add(map);
            }
        }
        return result;
    }
    public List<Map<String, Object>> readJson(MultipartFile file) throws IOException {
        String content = new String(file.getBytes(), StandardCharsets.UTF_8);
        try {
            return jsonMapper.readValue(content, List.class);
        } catch (Exception e) {
            Map<String, Object> single = jsonMapper.readValue(content, Map.class);
            return Collections.singletonList(single);
        }
    }
    public List<Map<String, Object>> readXml(MultipartFile file) throws IOException {
        String content = new String(file.getBytes(), StandardCharsets.UTF_8);
        Map<String, Object> data = xmlMapper.readValue(content, Map.class);
        if (data.containsKey("records")) {
            Object records = data.get("records");
            if (records instanceof List) {
                return (List<Map<String, Object>>) records;
            } else if (records instanceof Map) {
                return Collections.singletonList((Map<String, Object>) records);
            }
        }
        return Collections.singletonList(data);
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
    private void extractFeatures(List<Feature> features, List<Map<String, Object>> result, String path){
        if (features == null) return;
        for (Feature feature: features){
            if(feature instanceof Placemark){
                Map<String, Object> record = convertPlacemarkToMap((Placemark) feature, path);
                result.add(record);
            }else if(feature instanceof Folder folder){
                String newPath = path + (path.isEmpty() ? "" : "/") +
                        (folder.getName() != null ? folder.getName() : "Unnamed");
                extractFeatures(folder.getFeature(), result, newPath);
            }else if(feature instanceof Document doc){
                extractFeatures(doc.getFeature(), result, path);
            }
        }
    }
    private Map<String, Object> convertPlacemarkToMap(Placemark placemark, String path){
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("name", placemark.getName() != null ? placemark.getName() : "");
        record.put("description", placemark.getDescription() != null ? placemark.getDescription(): "");
        record.put("folder_path", path);

        if(placemark.getGeometry() != null){
            Object geometry = placemark.getGeometry();
            record.put("geometry_type", geometry.getClass().getSimpleName());

            if(geometry instanceof Point){
                Point point = (Point) placemark.getGeometry();
                if(point.getCoordinates() != null && !point.getCoordinates().isEmpty()){
                    List<Coordinate> coords = point.getCoordinates();
                    if(!coords.isEmpty()){
                        Coordinate coord = coords.get(0);
                        record.put("longitude", coord.getLongitude());
                        record.put("latitude", coord.getLatitude());
                        record.put("altitude", coord.getAltitude());
                    }
                }
            }else if (geometry instanceof LineString line){
                if(line.getCoordinates() != null){
                    record.put("coordinates", coordinatesToString(line.getCoordinates()));
                }
            }else if(geometry instanceof Polygon polygon){
                if(polygon.getOuterBoundaryIs() != null &&
                    polygon.getOuterBoundaryIs().getLinearRing() != null &&
                    polygon.getOuterBoundaryIs().getLinearRing().getCoordinates() != null){
                    record.put("outer_boundary", coordinatesToString(
                            polygon.getOuterBoundaryIs().getLinearRing().getCoordinates()));
                }
            }
        }
        return record;
    }
    private String coordinatesToString(List<Coordinate> coordinates){
        if(coordinates == null || coordinates.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for(Coordinate coordinate: coordinates){
            sb.append(coordinate.getLongitude())
                    .append(",")
                    .append(coordinate.getLatitude());
            if(coordinate.getAltitude() != 0.0) sb.append(",").append(coordinate.getAltitude());
            sb.append(" ");
        }
        return sb.toString().trim();
    }
}
