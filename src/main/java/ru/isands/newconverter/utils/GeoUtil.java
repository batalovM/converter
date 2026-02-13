package ru.isands.newconverter.utils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.geotools.api.feature.Property;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.kml.v22.KML;
import org.geotools.kml.v22.KMLConfiguration;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.wfs.GML;
import org.geotools.xsd.Encoder;
import org.geotools.xsd.PullParser;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import ru.isands.newconverter.enums.Format;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author batal
 * @Date 12.02.2026
 */
@Component
public class GeoUtil {
    @Value("${app.temp-dir}")
    private String tempDir;
    private static final int DECIMALS = 15;
    public Resource kmlToGeoJson(MultipartFile file) throws Exception {
        SimpleFeatureCollection fc = parseKml(file);
        return writeToGeoJson(fc);
    }
    public Resource kmlToCsv(MultipartFile file) throws Exception {
        SimpleFeatureCollection fc = parseKml(file);
        return writeToCsv(fc);
    }
    public Resource kmlToGml(MultipartFile file) throws Exception {
        SimpleFeatureCollection fc = parseKml(file);
        return writeToGml(fc);
    }
    public Resource geoJsonToKml(MultipartFile file) throws Exception {
        SimpleFeatureCollection fc = readGeoJson(file);
        return writeToKml(fc);
    }
    public Resource csvToKml(MultipartFile file) throws Exception {
        SimpleFeatureCollection fc = readCsvAsFeatures(file);
        return writeToKml(fc);
    }
    public Resource gmlToKml(MultipartFile file) throws Exception {
        SimpleFeatureCollection fc = readGmlAsFeatures(file);
        return writeToKml(fc);
    }
    public SimpleFeatureCollection parseKml(MultipartFile file) throws Exception {
        DefaultFeatureCollection fc = new DefaultFeatureCollection();
        try (InputStream is = new BufferedInputStream(file.getInputStream())) {
            PullParser parser = new PullParser(new KMLConfiguration(), is, KML.Placemark);
            SimpleFeature f;
            while ((f = (SimpleFeature) parser.parse()) != null) {
                fc.add(f);
            }
        }
        return fc;
    }
    public SimpleFeatureCollection readGeoJson(MultipartFile file) throws IOException {
        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(DECIMALS));
        try (InputStream is = new BufferedInputStream(file.getInputStream())) {
            return (SimpleFeatureCollection) featureJSON.readFeatureCollection(is);
        }
    }
    public SimpleFeatureCollection readCsvAsFeatures(MultipartFile file) throws Exception {
        try (Reader reader = new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8);
             CSVReader csvReader = new CSVReader(reader)) {
            String[] headers = csvReader.readNext();
            if (headers == null) {
                throw new IllegalArgumentException("CSV file is empty — no headers found");
            }
            int wktIndex = findWktColumn(headers);
            if (wktIndex < 0) {
                throw new IllegalArgumentException(
                        "CSV file must contain a geometry column named 'WKT', 'THE_GEOM', or 'GEOMETRY'");
            }
            SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
            typeBuilder.setName("CsvFeatures");
            typeBuilder.setCRS(DefaultGeographicCRS.WGS84);
            typeBuilder.add("geometry", Geometry.class);
            for (int i = 0; i < headers.length; i++) {
                if (i != wktIndex) {
                    typeBuilder.add(headers[i], String.class);
                }
            }
            SimpleFeatureType featureType = typeBuilder.buildFeatureType();
            SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
            List<SimpleFeature> features = new ArrayList<>();
            WKTReader wktReader = new WKTReader();
            String[] row;
            int id = 0;
            while ((row = csvReader.readNext()) != null) {
                Geometry geom = null;
                if (wktIndex < row.length && !row[wktIndex].isBlank()) {
                    try {
                        geom = wktReader.read(row[wktIndex]);
                    } catch (Exception ignored) {
                    }
                }
                featureBuilder.set("geometry", geom);

                for (int i = 0; i < headers.length; i++) {
                    if (i != wktIndex) {
                        String value = i < row.length ? row[i] : "";
                        featureBuilder.set(headers[i], value);
                    }
                }
                features.add(featureBuilder.buildFeature(String.valueOf(id++)));
            }
            return new ListFeatureCollection(featureType, features);
        }
    }
    public SimpleFeatureCollection readGmlAsFeatures(MultipartFile file) throws Exception {
        GML gml = new GML(GML.Version.WFS1_1);
        try (InputStream is = file.getInputStream()) {
            return gml.decodeFeatureCollection(is);
        }
    }
    public Resource writeToKml(SimpleFeatureCollection fc) throws IOException {
        File tempFile = File.createTempFile("kml_", Format.KML.getSuffix(), new File(tempDir));
        SimpleFeatureCollection wgs84 = ensureWgs84(fc);

        Encoder encoder = new Encoder(new KMLConfiguration());
        encoder.setIndenting(true);

        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            encoder.encode(wgs84, KML.kml, output);
        }
        return new UrlResource(tempFile.toURI());
    }
    public Resource writeToGeoJson(SimpleFeatureCollection fc) throws IOException {
        File tempFile = File.createTempFile("geojson_", Format.GEOJSON.getSuffix(), new File(tempDir));
        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(DECIMALS));
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            featureJSON.writeFeatureCollection(fc, fos);
        }
        return new UrlResource(tempFile.toURI());
    }
    public Resource writeToCsv(SimpleFeatureCollection fc) throws IOException {
        File tempFile = File.createTempFile("csv_", Format.CSV.getSuffix(), new File(tempDir));

        try (FileWriter fileWriter = new FileWriter(tempFile, StandardCharsets.UTF_8);
             CSVWriter writer = new CSVWriter(fileWriter)) {
            SimpleFeatureType schema = fc.getSchema();
            List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
            List<String> headers = new ArrayList<>();
            for (AttributeDescriptor desc : descriptors) {
                if (desc instanceof GeometryDescriptor) {
                    headers.add("WKT");
                } else {
                    headers.add(desc.getLocalName());
                }
            }
            writer.writeNext(headers.toArray(new String[0]));
            WKTWriter wktWriter = new WKTWriter();
            try (SimpleFeatureIterator iterator = fc.features()) {
                while (iterator.hasNext()) {
                    SimpleFeature feature = iterator.next();
                    List<String> row = new ArrayList<>();
                    for (Property property : feature.getProperties()) {
                        Object value = property.getValue();
                        if (value == null) {
                            row.add("");
                        } else if (value instanceof Geometry geom) {
                            row.add(wktWriter.write(geom));
                        } else {
                            row.add(value.toString());
                        }
                    }
                    writer.writeNext(row.toArray(new String[0]));
                }
            }
        }
        return new UrlResource(tempFile.toURI());
    }
    public Resource writeToGml(SimpleFeatureCollection fc) throws IOException {
        File tempFile = File.createTempFile("gml_", Format.GML.getSuffix(), new File(tempDir));
        GML encode = new GML(GML.Version.WFS1_1);
        encode.setNamespace("feature", "http://example.com/feature");
        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            encode.encode(output, fc);
        }
        return new UrlResource(tempFile.toURI());
    }
    private SimpleFeatureCollection ensureWgs84(SimpleFeatureCollection fc) {
        if (fc.getSchema().getCoordinateReferenceSystem() == null) {
            return fc;
        }
        try {
            var targetCRS = CRS.decode("EPSG:4326");
            var sourceCRS = fc.getSchema().getCoordinateReferenceSystem();
            if (!CRS.equalsIgnoreMetadata(sourceCRS, targetCRS)) {
                return new ReprojectingFeatureCollection(fc, targetCRS);
            }
        } catch (Exception ignored) {
        }
        return fc;
    }
    private int findWktColumn(String[] headers) {
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim().toUpperCase();
            if (h.equals("WKT") || h.equals("THE_GEOM") || h.equals("GEOMETRY")) {
                return i;
            }
        }
        return -1;
    }
}