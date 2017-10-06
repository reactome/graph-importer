package org.reactome.server.graph.batchimport;

public class ReactomeAttribute {

    private String attribute;
    private PropertyType type;

    public enum PropertyType {
        //                  Allows
        //               null   empty
        MANDATORY       (false, false),     // It must be filled in
        OPTIONAL        (true,  true),      // The curator gets to decide whether it is included
        REQUIRED        (true,  false),     // If it is relevant it MUST be included
        NOMANUALEDIT    (true,  true);      // The curator tool or release process will fill it in

        PropertyType(boolean allowsNull, boolean allowsEmpty) {
            this.allowsNull = allowsNull;
            this.allowsEmpty = allowsEmpty;
        }

        public final boolean allowsNull;
        public final boolean allowsEmpty;
    }

    ReactomeAttribute(String attribute, PropertyType type) {
        this.attribute = attribute;
        this.type = type;
    }

    String getAttribute() {
        return attribute;
    }

    PropertyType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReactomeAttribute that = (ReactomeAttribute) o;

        //noinspection SimplifiableIfStatement
        if (attribute != null ? !attribute.equals(that.attribute) : that.attribute != null) return false;
        return type != null ? type.equals(that.type) : that.type == null;
    }

    @Override
    public int hashCode() {
        int result = attribute != null ? attribute.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
