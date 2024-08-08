package au.csiro.pathling.fhirpath;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import jakarta.annotation.Nullable;
import java.io.IOException;

public class FhirPathGsonTypeAdapter extends TypeAdapter<FhirPath> {

  private final Gson gson;

  public FhirPathGsonTypeAdapter(final Gson gson) {
    this.gson = gson;
  }

  @Override
  public void write(@Nullable final JsonWriter out, @Nullable final FhirPath value)
      throws IOException {
    if (out == null) {
      return;
    }
    if (value == null) {
      out.nullValue();
      return;
    }
    out.jsonValue(value.toJson(gson));
  }

  @Override
  public FhirPath read(final JsonReader in) {
    throw new UnsupportedOperationException("Does not support reading");
  }

}
