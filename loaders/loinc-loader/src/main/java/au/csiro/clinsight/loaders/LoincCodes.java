/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.loaders;

import au.csiro.clinsight.TerminologyClient;

import java.util.Iterator;

public class LoincCodes implements Iterable<LoincCode> {

    private LoincCodesIterator loincCodesIterator;

    public LoincCodes(TerminologyClient terminologyClient, int pageSize) {
        this.loincCodesIterator = new LoincCodesIterator(terminologyClient, pageSize);
    }

    public int getTotal() {
        return loincCodesIterator.getTotal();
    }

    public int getCurrentIndex() {
        return loincCodesIterator.getCurrentIndex();
    }

    @Override
    public Iterator<LoincCode> iterator() {
        return loincCodesIterator;
    }

}
