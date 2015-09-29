package org.marksup.engine.spark.sql.udf;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ParseCoordinates extends AbstractFunction2<Float, Float, List<Float>> implements Serializable {

    private static final long serialVersionUID = -1494997808699658439L;

    public ParseCoordinates() {
        super();
    }

    @Override
    public List<Float> apply(Float lat, Float lon) {
        if (lat == null || lon == null) {
            return Arrays.asList((float) 0, (float) 0);
        }
        // Format in [lon, lat], note, the order of lon/lat here in order to conform with GeoJSON.
        // https://www.elastic.co/guide/en/elasticsearch/reference/1.3/mapping-geo-point-type.html#_lat_lon_as_array_5
        return Arrays.asList(lon, lat);
    }
}