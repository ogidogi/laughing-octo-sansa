package org.marksup.engine.spark.sql.udf;

import eu.bitwalker.useragentutils.UserAgent;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

import org.marksup.engine.utils.MapperConstants;

public class ParseUserAgentString extends AbstractFunction1<String, Object> implements Serializable {

    private static final long serialVersionUID = -1474997808699658439L;
    private static final String DFLT_NULL = null;

    private final MapperConstants.SchemaFields userAgentComponent;

    public ParseUserAgentString(MapperConstants.SchemaFields userAgentComponent) {
        this.userAgentComponent = userAgentComponent;
    }

    @Override
    public Object apply(String value) {
        final UserAgent userAgent = UserAgent.parseUserAgentString(value);
        switch (userAgentComponent) {
        case UA_BROWSER:
            return userAgent.getBrowser() != null ? String.valueOf(userAgent.getBrowser()) : DFLT_NULL;
        case UA_BROWSER_TYPE:
            return userAgent.getBrowser() != null ? String.valueOf(userAgent.getBrowser().getBrowserType()) : DFLT_NULL;
        case UA_BROWSER_MANUFACTURER:
            return userAgent.getBrowser() != null ? String.valueOf(userAgent.getBrowser().getManufacturer()) : DFLT_NULL;
        case UA_BROWSER_GROUP:
            return userAgent.getBrowser() != null ? String.valueOf(userAgent.getBrowser().getGroup()) : DFLT_NULL;
        case UA_BROWSER_RENDERING_ENGINE:
            return userAgent.getBrowser() != null ? String.valueOf(userAgent.getBrowser().getRenderingEngine()) : DFLT_NULL;
        case UA_BROWSERVERSION:
            //TODO changed in H2O model to String
            return userAgent.getBrowserVersion() != null ? Double.parseDouble(userAgent.getBrowserVersion().toString()) : DFLT_NULL;
        case UA_BROWSERVERSION_MINOR:
            return userAgent.getBrowserVersion() != null ? Integer.parseInt(userAgent.getBrowserVersion().getMinorVersion()) : DFLT_NULL;
        case UA_BROWSERVERSION_MAJOR:
            return userAgent.getBrowserVersion() != null ? Integer.parseInt(userAgent.getBrowserVersion().getMajorVersion()) : DFLT_NULL;
        case UA_ID:
            return userAgent.getId();
        case UA_OS:
            return userAgent.getOperatingSystem() != null ? String.valueOf(userAgent.getOperatingSystem()) : DFLT_NULL;
        case UA_OS_NAME:
            return userAgent.getOperatingSystem() != null ? String.valueOf(userAgent.getOperatingSystem().getName()) : DFLT_NULL;
        case UA_OS_DEVICE:
            return userAgent.getOperatingSystem() != null ? String.valueOf(userAgent.getOperatingSystem().getDeviceType()) : DFLT_NULL;
        case UA_OS_GROUP:
            return userAgent.getOperatingSystem() != null ? String.valueOf(userAgent.getOperatingSystem().getGroup()) : DFLT_NULL;
        case UA_OS_MANUFACTURER:
            return userAgent.getOperatingSystem() != null ? String.valueOf(userAgent.getOperatingSystem().getManufacturer()) : DFLT_NULL;
        default:
            return null;
        }
    }

}
