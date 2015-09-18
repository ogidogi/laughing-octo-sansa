package org.bidsup.engine.spark.sql.udf;

import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ParseUserTagsArray extends AbstractFunction1<String, List<String>> implements Serializable {

    private static final long serialVersionUID = -1484997808699658439L;
    private static final String DFTL_DELIMITER = ",";

    private String delimiter;

    public ParseUserTagsArray() {
        super();
        delimiter = DFTL_DELIMITER;
    }

    public ParseUserTagsArray(String delim) {
        super();
        delimiter = delim;
    }

    @Override
    public List<String> apply(String value) {
        if (value == null) {
            return null;    //Arrays.asList("0");
        }
        return Arrays.stream(value.split(delimiter)).collect(Collectors.toList());
    }
}
