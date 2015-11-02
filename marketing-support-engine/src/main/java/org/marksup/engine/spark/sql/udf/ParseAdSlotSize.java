package org.marksup.engine.spark.sql.udf;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;

public class ParseAdSlotSize extends AbstractFunction2<Integer, Integer, String> implements Serializable {
    private static final long serialVersionUID = -1494997808699658439L;

    public ParseAdSlotSize() {
        super();
    }

    @Override
    public String apply(Integer width, Integer height) {
        if (width == null || height == null) {
            return "Unknown";
        }
        return width + "x" + height;
    }
}
