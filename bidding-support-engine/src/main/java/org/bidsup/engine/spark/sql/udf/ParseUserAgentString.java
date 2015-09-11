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
            case "browser.type":
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getBrowserType() : "Unknown");
            case "browser.manufacturer":
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getManufacturer() : "Unknown");
            case "browser.group":
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getGroup() : "Unknown");
            case "browser.rendering.engine":
                return String.valueOf(userAgent.getBrowser() != null ? userAgent.getBrowser().getRenderingEngine() : "Unknown");

            case "browserVersion":
                return String.valueOf(userAgent.getBrowserVersion());
            case "browserVersion.minor":
                return String.valueOf(userAgent.getBrowserVersion() != null ? userAgent.getBrowserVersion().getMinorVersion() : "Unknown");
            case "browserVersion.major":
                return String.valueOf(userAgent.getBrowserVersion() != null ? userAgent.getBrowserVersion().getMajorVersion() : "Unknown");

            case "id":
                return String.valueOf(userAgent.getId());

            case "operatingSystem":
                return String.valueOf(userAgent.getOperatingSystem());
            case "operatingSystem.name":
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getName() : "Unknown");
            case "operatingSystem.device":
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getDeviceType() : "Unknown");
            case "operatingSystem.group":
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getGroup() : "Unknown");
            case "operatingSystem.manufacturer":
                return String.valueOf(userAgent.getOperatingSystem() != null ? userAgent.getOperatingSystem().getManufacturer() : "Unknown");

            default:
                return null;
        }
    }

}
