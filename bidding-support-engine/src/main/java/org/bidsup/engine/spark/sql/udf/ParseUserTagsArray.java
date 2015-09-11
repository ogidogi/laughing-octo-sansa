package org.bidsup.engine.spark.sql.udf;

import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ParseUserTagsArray extends AbstractFunction1<String, List<String>> implements Serializable {

    private static final long serialVersionUID = -1484997808699658439L;

    public ParseUserTagsArray() {
        super();
    }

    @Override
    public List<String> apply(String value) {
        //        return Arrays.stream(value.split(",")).map(x -> Integer.parseInt(x)).collect(Collectors.toList());
        if (value == null) {
            return Arrays.asList("0");
        }
        return Arrays.stream(value.split(",")).collect(Collectors.toList());
    }
}
