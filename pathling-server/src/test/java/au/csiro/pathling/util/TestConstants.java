package au.csiro.pathling.util;

import java.util.function.Function;

/**
 * @author Felix Naumann
 */
public class TestConstants {

    public static final String WAREHOUSE_PLACEHOLDER = "WAREHOUSE_PATH";

    public static final Function<String, String> RESOLVE_PATIENT = string -> string + "/Patient.ndjson";
    public static final int PATIENT_COUNT = 100;

    public static final Function<String, String> RESOLVE_ENCOUNTER = string -> string + "/Encounter.ndjson";
    public static final int ENCOUNTER_COUNT = 3697;
}
