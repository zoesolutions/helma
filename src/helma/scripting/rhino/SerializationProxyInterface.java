/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is available at
 * http://adele.helma.org/download/helma/license.txt
 *
 * Copyright 2008 Helma Software. All Rights Reserved.
 */

package helma.scripting.rhino;

import java.io.Serializable;

import org.mozilla.javascript.Context;

import helma.objectmodel.NodeInterface;
import helma.objectmodel.db.Node;
import helma.objectmodel.db.NodeHandle;

/**
 * Serialization proxy/placeholder interface. This is used for
 * for various Helma and Rhino related classes..
 */
public interface SerializationProxyInterface extends Serializable {
    public Object getObject(RhinoEngine engine);
}

/**
 * Serialization proxy for app, req, res, path objects.
 */
class ScriptBeanProxy implements SerializationProxyInterface {
    private static final long serialVersionUID = -1002489933060844917L;

    String name;

    ScriptBeanProxy(String name) {
        this.name = name;
    }

    /**
     * Lookup the actual object in the current scope
     *
     * @return the object represented by this proxy
     */
    public Object getObject(RhinoEngine engine) {
        return engine.global.get(this.name, engine.global);
    }
}

/**
 * Serialization proxy for the application object.
 *
 * @author Daniel Ruthardt
 * @since 20170918
 */
class ApplicationProxy implements SerializationProxyInterface {
    private static final long serialVersionUID = -3635418002212260600L;

    public Object getObject(RhinoEngine engine) {
        // return the application
        return engine.app;
    }
}

/**
 * Serialization proxy for global scope
 */
class GlobalProxy implements SerializationProxyInterface {
    private static final long serialVersionUID = -3200125667487274257L;

    boolean shared;

    GlobalProxy(GlobalObject scope) {
        this.shared = !scope.isThreadScope;
    }

    /**
     * Lookup the actual object in the current scope
     *
     * @return the object represented by this proxy
     */
    public Object getObject(RhinoEngine engine) {
        return this.shared ? engine.core.global : engine.global;
    }
}

/**
 * Serialization proxy for various flavors of HopObjects/Nodes
 */
class HopObjectProxy implements SerializationProxyInterface {
    private static final long serialVersionUID = -4808579296683836009L;

    Object ref;
    boolean wrapped = false;

    HopObjectProxy(HopObject obj) {
        NodeInterface n = obj.getNode();
        if (n == null) {
            this.ref = obj.getClassName();
        } else {
            if (n instanceof Node) {
                this.ref = ((Node) n).getHandle();
            } else {
                this.ref = n;
            }
        }
        this.wrapped = true;
    }

    HopObjectProxy(Node node) {
        this.ref = node.getHandle();
    }

    /**
     * Lookup the actual object in the current scope
     *
     * @return the object represented by this proxy
     */
    public Object getObject(RhinoEngine engine) {
        if (this.ref instanceof String)
            return engine.core.getPrototype((String) this.ref);
        else if (this.ref instanceof NodeHandle) {
            Object n = ((NodeHandle) this.ref).getNode(engine.app.getWrappedNodeManager());
            if (n == null) {
                return null;
            }
            return this.wrapped ? Context.toObject(n, engine.global) : n;
        }
        return Context.toObject(this.ref, engine.global);
    }

}
