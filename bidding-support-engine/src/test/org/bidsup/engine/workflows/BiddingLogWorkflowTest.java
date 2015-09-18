package org.bidsup.engine.workflows;

import junit.framework.TestCase;

public class BiddingLogWorkflowTest extends TestCase {
    public void testGetDateFromName() throws Exception {
        String name = "site-click.20130606-aa.txt";
        String date = BiddingLogWorkflow.getDateFromName(name);
        System.out.println("date = " + date);
        assertEquals(date, "20130606");
    }

    public void testGetEsIdxFromName() throws Exception {
        String name = "site-click.20130606-aa.txt";
        String esIdxSuffix = "log_%s/bid";
        String idx = BiddingLogWorkflow.getEsIdxFromName(name, esIdxSuffix);
        System.out.println("idx = " + idx);
        assertEquals(idx, "log_20130606/bid");
    }
}