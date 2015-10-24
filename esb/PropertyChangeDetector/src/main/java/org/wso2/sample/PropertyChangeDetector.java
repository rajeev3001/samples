package org.wso2.sample;

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.AbstractMediator;

/**
 * Detects a change in a specified property and sets an indicator value to another property.
 */
public class PropertyChangeDetector extends AbstractMediator {

    static String LOOKUP_PROPERTY_NAME = "current_indicator_value";
    static String OUTPUT_PROPERTY_NAME = "indicator_changed";
    private Object oldValue = null;

    @Override
    public boolean mediate(MessageContext messageContext) {
        Object currentValue = messageContext.getProperty(LOOKUP_PROPERTY_NAME);
        System.out.println("PropertyChangeDetector mediator - current value: " + currentValue);
        if (currentValue !=  null && (!currentValue.equals(oldValue))) {
            messageContext.setProperty(OUTPUT_PROPERTY_NAME, "true");
            oldValue = currentValue;
        } else {
            messageContext.setProperty(OUTPUT_PROPERTY_NAME, "false");
        }
        return true;
    }
}