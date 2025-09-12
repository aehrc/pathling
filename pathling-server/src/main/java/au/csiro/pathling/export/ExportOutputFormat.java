package au.csiro.pathling.export;

/**
 * @author Felix Naumann
 */
public enum ExportOutputFormat {
    ND_JSON;

    public static String asParam(ExportOutputFormat exportOutputFormat) {
        return switch (exportOutputFormat) {
            case ND_JSON -> "ndjson";
        };
    }
}
