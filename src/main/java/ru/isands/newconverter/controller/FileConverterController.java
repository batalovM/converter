package ru.isands.newconverter.controller;
import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.isands.newconverter.enums.Format;
import ru.isands.newconverter.service.ParquetConverterService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Parameter;

@RestController
@RequestMapping("/api/convert")
@Tag(name = "Конвертер", description = "Конвертация файлов между Parquet/CSV/JSON/XML")
public class FileConverterController {

    private final ParquetConverterService converterService;

    public FileConverterController(ParquetConverterService converterService) {
        this.converterService = converterService;
    }
    @PostMapping(value = "/json", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(description = "parquet -> json")
    public ResponseEntity<Resource> getJson(@Parameter(description = "Файл (входной)") @RequestParam("file") MultipartFile file) throws Exception{
        Resource json = converterService.convertJson(file, Format.PARQUET);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=file.json")
                .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                .body(json);
    }
    @Operation(description = "parquet -> csv")
    @PostMapping(value = "/csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Resource> getCsv(@Parameter(description = "Файл (входной)") @RequestParam("file") MultipartFile file) throws Exception{
        Resource csv = converterService.convertCsv(file, Format.PARQUET);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=file.csv")
                .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                .body(csv);
    }
    @PostMapping(value = "/xml", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(description = "parquet -> xml")
    public ResponseEntity<Resource> getXml(@Parameter(description = "Файл (входной)") @RequestParam("file") MultipartFile file) throws Exception{
        Resource csv = converterService.convertXml(file, Format.PARQUET);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=file.xml")
                .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                .body(csv);
    }
    @PostMapping(value = "/parquet", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(description = "any -> parquet")
    public ResponseEntity<Resource> getParquet(
            @Parameter(description = "Файл (входной)") @RequestParam("file") MultipartFile file,
            @Parameter(description = "Формат входного файла", required = true,
                    schema = @Schema(type = "string", allowableValues = {"CSV", "JSON", "XML"}, defaultValue = "CSV"))
            @RequestParam String format) throws Exception{
        Resource csv = converterService.convertParquet(file, Format.valueOf(format));
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=file.parquet")
                .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                .body(csv);
    }
}
