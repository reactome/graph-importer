package org.reactome.server.graph.batchimport;

import java.util.Objects;

public class ReactomeAttribute {

    private final String attribute;
    private final PropertyType type;
    private final Class<?> clazz;
    private final Class<?> parent;

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
    ReactomeAttribute(String attribute, PropertyType type, Class<?> clazz, Class<?> parent) {
        this.attribute = attribute;
        this.type = type;
        this.clazz = clazz;
        this.parent = parent;
    }

    String getAttribute() {
        return attribute;
    }

    PropertyType getType() {
        return type;
    }

    public Class<?> getParent() {
        return parent;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReactomeAttribute attribute1 = (ReactomeAttribute) o;
        return Objects.equals(attribute, attribute1.attribute) && type == attribute1.type && Objects.equals(parent, attribute1.parent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attribute, type, parent);
    }
}
