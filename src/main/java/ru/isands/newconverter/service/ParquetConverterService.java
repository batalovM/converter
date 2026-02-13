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
        List<Map<String, Object>> data = readData(inputFile, inputFormat);
        return writeUtil.writeToJson(data);
    }
    public Resource convertCsv(MultipartFile inputFile, Format inputFormat) {
        List<Map<String, Object>> data = readData(inputFile, inputFormat);
        return writeUtil.writeToCsv(data);
    }
    public Resource convertXml(MultipartFile inputFile, Format inputFormat) {
        List<Map<String, Object>> data = readData(inputFile, inputFormat);
        return writeUtil.writeToXml(data);
    }
    public Resource convertParquet(MultipartFile inputFile, Format inputFormat) {
        List<Map<String, Object>> data = readData(inputFile, inputFormat);
        return writeUtil.writeToParquet(data);
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