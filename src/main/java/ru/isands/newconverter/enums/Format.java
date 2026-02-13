package ru.isands.newconverter.enums;

/**
 * @author batal
 * @Date 06.02.2026
 */
public enum Format {
    PARQUET(".parquet"),
    CSV(".csv"),
    JSON(".json"),
    XML(".xml"),
    KML(".kml"),
    GEOJSON(".geojson"),
    GML(".gml");
    private final String suffix;

    Format(String suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        return suffix;
    }
}