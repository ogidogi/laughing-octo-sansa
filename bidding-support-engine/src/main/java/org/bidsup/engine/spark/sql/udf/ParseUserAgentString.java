package org.bidsup.engine.spark.sql.udf;

import eu.bitwalker.useragentutils.UserAgent;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

public class ParseUserAgentString extends AbstractFunction1<String, String> implements Serializable {

    private static final long serialVersionUID = -1474997808699658439L;

    private final String userAgentComponent;

    public ParseUserAgentString(String userAgentComponent) {
        this.userAgentComponent = userAgentComponent;
    }

    @Override
    public String apply(String value) {
        final UserAgent userAgent = UserAgent.parseUserAgentString(value);
        switch (userAgentComponent) {
            case "browser":
                return String.valueOf(userAgent.getBrowser());
            case "browserVersion":
                return String.valueOf(userAgent.getBrowserVersion());
            case "id":
                return String.valueOf(userAgent.getId());
            case "operatingSystem":
                return String.valueOf(userAgent.getOperatingSystem());
            default:
                return null;
        }
    }

}
