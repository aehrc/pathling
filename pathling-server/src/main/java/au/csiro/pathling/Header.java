package au.csiro.pathling;

import java.util.List;

/**
 * @author Felix Naumann
 */
public record Header(
        String headerName,
        List<String> acceptedHeaderValues
) {

    public String preferred() {
        return acceptedHeaderValues().get(0);
    }

    public boolean validValue(String headerValue) {
        return headerValue != null && acceptedHeaderValues.contains(headerValue);
    }
}
