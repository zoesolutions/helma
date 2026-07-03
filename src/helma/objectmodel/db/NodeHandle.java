/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is available at
 * http://adele.helma.org/download/helma/license.txt
 *
 * Copyright 1998-2003 Helma Software. All Rights Reserved.
 */

package helma.objectmodel.db;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import helma.objectmodel.NodeInterface;

/**
 * This class is a handle or reference to a Node. This is to abstract from different
 * methods of reference: Transient Nodes are referred to directly, while persistent
 * nodes are referred to via key/node manager.
 *
 * A handle is used to refer to a node in a safe way over a longer period.
 * While a direct reference may point to a node that has been evicted from the cache
 * and reinstanciated since being set, NodeHandle will always return an up-to-date
 * instance of its node.
 *
 * Helma tries to ensure the following rules on NodeHandles:
 * <ol>
 * <li> For transient nodes there exists only one NodeHandle.</li>
 * <li> If a transient node becomes persistent its node handle is notified and
 *      converted into a persistent NodeHandle.</li>
 * </ol>
 * These two properties guarantee that NodeHandle comparisons are easy and usually correct.
 *
 */
public final class NodeHandle implements Serializable {
    static final long serialVersionUID = 3067763116576910931L;

    // direct reference to the node
    private transient Node node;

    // the node's key
    private KeyInterface key;

    /**
     * Builds a handle for a node. This constructor is package private in order to make
     * sure only one NodeHandle exists per transient node. Use {@link Node#getHandle()}
     * to get a Node's handle.
     * @param node the node
     */
    NodeHandle(Node node) {
        int state = node.getState();

        if (state == NodeInterface.TRANSIENT) {
            this.node = node;
            this.key = null;
        } else {
            this.node = null;
            this.key = node.getKey();
        }
    }

    /**
     * Builds a handle given a node's retrieval information. At the time this is called,
     * the node is ususally not yet created. It will be fetched on demand when accessed by
     * application code.
     * @param key the key
     */
    public NodeHandle(KeyInterface key) {
        this.node = null;
        this.key = key;
    }

    /**
     *  Get the node described by this node handle
     */
    public Node getNode(WrappedNodeManager nodemgr) {
        if (this.node != null) {
            return this.node;
        }
        if (this.key == null) {
            return null;
        }
        return nodemgr.getNode(this.key);
    }

    /**
     * Check if the node is available without fetching it from the node manager
     * @return true if we alreay have a reference to our node
     */
    public boolean hasNode() {
        return this.node != null;
    }

    /**
     *  Get the key for the node described by this handle.
     *  This will return null for transient Nodes.
     */
    public KeyInterface getKey() {
        return this.key;
    }

    /**
     *  Get the ID for the node described by this handle.
     *  This may only be called on persistent Nodes.
     */
    public String getID() {
        if (this.key == null) {
            return this.node == null ? null : this.node.getID();
        }
        return this.key.getID();
    }

    private Object getObject() {
        if (this.node != null) {
            return this.node;
        }
        return this.key;
    }

    /**
     *
     *
     * @param other ...
     *
     * @return ...
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof NodeHandle) {
            Object obj1 = getObject();
            Object obj2 = ((NodeHandle) other).getObject();
            return obj1 == obj2 || (obj1 != null && obj1.equals(obj2));
        }
        return false;
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException {
        if (this.key == null && this.node != null) {
            throw new NotSerializableException("Transient NodeHandle cannot be serialized");
        }
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.node = null;
    }

    /**
     * This is to notify the handle that the underlying node is becoming
     * persistent and we have to refer to it via the key from now on.
     */
    protected void becomePersistent() {
        if (this.node != null) {
            this.key = this.node.getKey();
            this.node = null;
        }
    }

    /**
     *
     *
     * @return ...
     */
    @Override
    public String toString() {
        if (this.node != null) {
            return "NodeHandle[transient:" + this.node + "]";  //$NON-NLS-1$//$NON-NLS-2$
        }
        return "NodeHandle[" + this.key + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}
