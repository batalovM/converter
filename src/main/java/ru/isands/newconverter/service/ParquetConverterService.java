package ru.isands.newconverter.service;

import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import ru.isands.newconverter.enums.Format;
import ru.isands.newconverter.exception.ConversionException;
import ru.isands.newconverter.utils.ReadUtil;
import ru.isands.newconverter.utils.WriteUtil;

import java.util.*;

@Service
public class ParquetConverterService {
    private final ReadUtil readUtil;
    private final WriteUtil writeUtil;
    public ParquetConverterService(ReadUtil readUtil, WriteUtil writeUtil) {
        this.readUtil = readUtil;
        this.writeUtil = writeUtil;
    }
    public Resource convertJson(MultipartFile inputFile, Format inputFormat) {
        try {
            List<Map<String, Object>> data = readData(inputFile, inputFormat);
            return writeUtil.writeToJson(data);
        } catch (Exception e) {
            throw new ConversionException("Failed to convert to JSON: " + e.getMessage(), e);
        }
    }
    public Resource convertCsv(MultipartFile inputFile, Format inputFormat) {
        try {
            List<Map<String, Object>> data = readData(inputFile, inputFormat);
            return writeUtil.writeToCsv(data);
        } catch (Exception e) {
            throw new ConversionException("Failed to convert to CSV: " + e.getMessage(), e);
        }
    }
    public Resource convertXml(MultipartFile inputFile, Format inputFormat) {
        try {
            List<Map<String, Object>> data = readData(inputFile, inputFormat);
            return writeUtil.writeToXml(data);
        } catch (Exception e) {
            throw new ConversionException("Failed to convert to XML: " + e.getMessage(), e);
        }
    }
    public Resource convertParquet(MultipartFile inputFile, Format inputFormat) {
        try {
            List<Map<String, Object>> data = readData(inputFile, inputFormat);
            return writeUtil.writeToParquet(data);
        } catch (Exception e) {
            throw new ConversionException("Failed to convert to Parquet: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> readData(MultipartFile file, Format format) {
        return switch (format) {
            case PARQUET -> readUtil.readParquet(file);
            case CSV -> readUtil.readCsv(file);
            case JSON -> readUtil.readJson(file);
            case XML -> readUtil.readXml(file);
        };
    }
}