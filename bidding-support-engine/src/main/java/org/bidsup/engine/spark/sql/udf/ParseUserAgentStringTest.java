package org.bidsup.engine.spark.sql.udf;

import eu.bitwalker.useragentutils.UserAgent;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;

public class ParseUserAgentStringTest extends TestCase {

    public void testApply() throws Exception {
    }

    public void testPrinter() throws Exception {
        List<String> ua = Arrays.asList(
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
                "Mozilla/5.0 (Linux; U; Android 4.0.4; zh-cn; 7266 Build/IMM76I) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
                //"K-TouchC986t_TD/1.0 Android 4.0.3 Release/10.01.2012 Browser/WAP2.0 appleWebkit/534.30",
                "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)",
                "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1"
                );


        System.out.println("BROWSER");
        printBrowserData(ua);

        System.out.println("------------------------------------");
        System.out.println("BROWSER VERSION");
        printBrowserVersionData(ua);

        System.out.println("------------------------------------");
        System.out.println("ID");
        printIdData(ua);

        System.out.println("------------------------------------");
        System.out.println("OS");
        printOsData(ua);
    }

    public void printBrowserData(List<String> agentData) {
        agentData.forEach(agent -> {
                UserAgent ua = UserAgent.parseUserAgentString(agent);
                System.out.println("------------------");
                System.out.println("agent = " + agent);
                System.out.println("userAgent.getBrowser() = " + ua.getBrowser());
                System.out.println("userAgent.getBrowser().getBrowserType() = " + ua.getBrowser().getBrowserType());
                System.out.println("userAgent.getBrowser().getManufacturer() = " + ua.getBrowser().getManufacturer());
                System.out.println("userAgent.getBrowser().getName() = " + ua.getBrowser().getName());
                System.out.println("userAgent.getBrowser().getGroup() = " + ua.getBrowser().getGroup());
                System.out.println("userAgent.getBrowser().getId() = " + ua.getBrowser().getId());
                System.out.println("userAgent.getBrowser().getRenderingEngine() = " + ua.getBrowser().getRenderingEngine());
            }
        );
    }

    public void printBrowserVersionData(List<String> agentData) {
        agentData.forEach(agent -> {
                UserAgent ua = UserAgent.parseUserAgentString(agent);
                System.out.println("------------------");
                System.out.println("agent = " + agent);
                System.out.println("ua.getBrowserVersion() = " + ua.getBrowserVersion());
                System.out.println("ua.getBrowserVersion().getMinorVersion() = " + ua.getBrowserVersion().getMinorVersion());
                System.out.println("ua.getBrowserVersion().getMajorVersion() = " + ua.getBrowserVersion().getMajorVersion());
            }
        );
    }

    public void printIdData(List<String> agentData) {
        agentData.forEach(agent -> {
                UserAgent ua = UserAgent.parseUserAgentString(agent);
                System.out.println("------------------");
                System.out.println("agent = " + agent);
                System.out.println("ua.getId() = " + ua.getId());
            }
        );
    }

    public void printOsData(List<String> agentData) {
        agentData.forEach(agent -> {
                UserAgent ua = UserAgent.parseUserAgentString(agent);
                System.out.println("------------------");
                System.out.println("agent = " + agent);
                System.out.println("ua.getOperatingSystem() = " + ua.getOperatingSystem());
                System.out.println("ua.getOperatingSystem().getName() = " + ua.getOperatingSystem().getName());
                System.out.println("ua.getOperatingSystem().getDeviceType() = " + ua.getOperatingSystem().getDeviceType());
                System.out.println("ua.getOperatingSystem().getGroup() = " + ua.getOperatingSystem().getGroup());
                System.out.println("ua.getOperatingSystem().getId() = " + ua.getOperatingSystem().getId());
                System.out.println("ua.getOperatingSystem().getManufacturer() = " + ua.getOperatingSystem().getManufacturer());
            }
        );
    }
}