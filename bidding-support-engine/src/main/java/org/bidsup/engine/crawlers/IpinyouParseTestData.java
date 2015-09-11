package org.bidsup.engine.crawlers;

import java.io.IOException;


public class IpinyouParseTestData {
    public static void main(String[] args) throws IOException {
        String esIndex = "rtb_test/log_item";
        String filePath = "/media/sf_Download/ipinyou/test";

        IpinyouParse parser = new IpinyouParse();
        parser.run(esIndex, filePath);
    }
}
