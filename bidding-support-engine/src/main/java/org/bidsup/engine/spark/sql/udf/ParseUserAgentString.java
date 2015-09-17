package org.bidsup.engine.spark.sql.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.bidsup.engine.utils.MapperConstants;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

public class ParseUserAgentString extends AbstractFunction1<String, String> implements Serializable {

    private static final long serialVersionUID = -1474997808699658439L;
    private static final String DFLT_UNKNOWN = "Unknown";

    private final MapperConstants.SchemaFields userAgentComponent;

    public ParseUserAgentString(MapperConstants.SchemaFields userAgentComponent) {
        this.userAgentComponent = userAgentComponent;
    }

    @Override
    public String apply(String value) {
        final UserAgent userAgent = UserAgent.parseUserAgentString(value);
        switch (userAgentComponent) {
            case UA_BROWSER:
                return String.valueOf(userAgent.getBrowser()!= null ? userAgent.getBrowser() : DFLT_UNKNOWN);
            case UA_BROWSER_TYPE:
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getBrowserType() : DFLT_UNKNOWN);
            case UA_BROWSER_MANUFACTURER:
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getManufacturer() : DFLT_UNKNOWN);
            case UA_BROWSER_GROUP:
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getGroup() : DFLT_UNKNOWN);
            case UA_BROWSER_RENDERING_ENGINE:
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getRenderingEngine() : DFLT_UNKNOWN);

            case UA_BROWSERVERSION:
                return String.valueOf(userAgent.getBrowserVersion() != null ? userAgent.getBrowserVersion() : DFLT_UNKNOWN);
            case UA_BROWSERVERSION_MINOR:
                return String.valueOf(userAgent.getBrowserVersion() != null ? userAgent.getBrowserVersion().getMinorVersion() : DFLT_UNKNOWN);
            case UA_BROWSERVERSION_MAJOR:
                return String.valueOf(userAgent.getBrowserVersion() != null ? userAgent.getBrowserVersion().getMajorVersion() : DFLT_UNKNOWN);

            case UA_ID:
                return String.valueOf(userAgent.getId());

            case UA_OS:
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem() : DFLT_UNKNOWN);
            case UA_OS_NAME:
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getName() : DFLT_UNKNOWN);
            case UA_OS_DEVICE:
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getDeviceType() : DFLT_UNKNOWN);
            case UA_OS_GROUP:
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getGroup() : DFLT_UNKNOWN);
            case UA_OS_MANUFACTURER:
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getManufacturer() : DFLT_UNKNOWN);

            default:
                return null;
        }
    }

}
