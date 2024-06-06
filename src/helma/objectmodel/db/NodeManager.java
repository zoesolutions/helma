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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import helma.framework.core.Application;
import helma.framework.core.RequestEvaluator;
import helma.objectmodel.DatabaseException;
import helma.objectmodel.DatabaseInterface;
import helma.objectmodel.NodeInterface;
import helma.objectmodel.ObjectCacheInterface;
import helma.objectmodel.TransactionInterface;
import helma.objectmodel.dom.XmlDatabase;

/**
 * The NodeManager is responsible for fetching Nodes from the internal or
 * external data sources, caching them in a least-recently-used Hashtable,
 * and writing changes back to the databases.
 */
public final class NodeManager {

    protected Application app;
    private ObjectCacheInterface cache;
    protected DatabaseInterface db;
    protected IDGeneratorInterface idgen;
    private boolean logSql;
    private Log sqlLog = null;
    private ArrayList listeners = new ArrayList();

    // a wrapper that catches some Exceptions while accessing this NM
    public final WrappedNodeManager safe;

    /**
     *  Create a new NodeManager for Application app.
     */
    public NodeManager(Application app) {
        this.app = app;
        this.safe = new WrappedNodeManager(this);
    }

    /**
     * Initialize the NodeManager for the given dbHome and
     * application properties. An embedded database will be
     * created in dbHome if one doesn't already exist.
     */
    public void init(File dbHome, Properties props)
            throws DatabaseException, ClassNotFoundException,
                   IllegalAccessException, InstantiationException {
        String cacheImpl = props.getProperty("cacheimpl", "helma.util.CacheMap");  //$NON-NLS-1$//$NON-NLS-2$

        this.cache = (ObjectCacheInterface) Class.forName(cacheImpl).newInstance();
        this.cache.init(this.app);

        String idgenImpl = props.getProperty("idGeneratorImpl"); //$NON-NLS-1$

        if (idgenImpl != null) {
            this.idgen = (IDGeneratorInterface) Class.forName(idgenImpl).newInstance();
            this.idgen.init(this.app);
        }

        this.logSql = "true".equalsIgnoreCase(props.getProperty("logsql"));  //$NON-NLS-1$//$NON-NLS-2$

        this.db = new XmlDatabase();
        this.db.init(dbHome, this.app);
    }

    /**
     * Gets the application's root node.
     */
    public Node getRootNode() throws Exception {
        DbMapping rootMapping = this.app.getRootMapping();
        DbKey key = new DbKey(rootMapping, this.app.getRootId());
        Node node = getNode(key);
        if (node != null && rootMapping != null) {
            node.setDbMapping(rootMapping);
            node.setPrototype(rootMapping.getTypeName());
        }
        return node;
    }
    /**
     * Checks if the given node is the application's root node.
     */
    public boolean isRootNode(Node node) {
        return node.getState() != NodeInterface.TRANSIENT && this.app.getRootId().equals(node.getID()) &&
               DbMapping.areStorageCompatible(this.app.getRootMapping(), node.getDbMapping());
    }

    /**
     *  app.properties file has been updated. Reread some settings.
     */
    public void updateProperties(Properties props) {
        // notify the cache about the properties update
        this.cache.updateProperties(props);
        this.logSql = "true".equalsIgnoreCase(props.getProperty("logsql"));  //$NON-NLS-1$//$NON-NLS-2$
    }

    /**
     *  Shut down this node manager. This is called when the application
     *  using this node manager is stopped.
     */
    public void shutdown() throws DatabaseException {
        this.db.shutdown();

        if (this.cache != null) {
            this.cache.shutdown();
            this.cache = null;
        }

        if (this.idgen != null) {
            this.idgen.shutdown();
        }
    }

    /**
     *  Delete a node from the database.
     */
    public void deleteNode(Node node) throws Exception {
        if (node != null) {
            synchronized (this) {
                Transactor tx = Transactor.getInstanceOrFail();

                node.setState(NodeInterface.INVALID);
                deleteNode(this.db, tx.txn, node);
            }
        }
    }

    /**
     *  Get a node by key. This is called from a node that already holds
     *  a reference to another node via a NodeHandle/KeyInterface.
     */
    public Node getNode(KeyInterface key) throws Exception {
        Transactor tx = Transactor.getInstanceOrFail();

        // See if Transactor has already come across this node
        Node node = tx.getCleanNode(key);

        if ((node != null) && (node.getState() != NodeInterface.INVALID)) {
            return node;
        }

        // try to get the node from the shared cache
        node = (Node) this.cache.get(key);

        if ((node == null) || (node.getState() == NodeInterface.INVALID)) {
            // The requested node isn't in the shared cache.
            if (key instanceof SyntheticKey) {
                Node parent = getNode(key.getParentKey());
                Relation rel = parent.dbmap.getPropertyRelation(key.getID());

                if (rel != null) {
                    return getNode(parent, key.getID(), rel);
                }
                return null;
            } else if (key instanceof DbKey) {
                node = getNodeByKey(tx.txn, (DbKey) key);
            }

            if (node != null) {
                node = registerNewNode(node, null);
            }
        }

        if (node != null) {
            tx.visitCleanNode(key, node);
        }

        return node;
    }

    /**
     *  Get a node by relation, using the home node, the relation and a key to apply.
     *  In contrast to getNode (KeyInterface key), this is usually called when we don't yet know
     *  whether such a node exists.
     */
    public Node getNode(Node home, String kstr, Relation rel)
                 throws Exception {
        if (kstr == null) {
            return null;
        }

        Transactor tx = Transactor.getInstanceOrFail();

        KeyInterface key;
        DbMapping otherDbm = rel == null ? null : rel.otherType;
        // check what kind of object we're looking for and make an appropriate key
        if (rel.isComplexReference()) {
            // a key for a complex reference
            key = new MultiKey(rel.otherType, rel.getKeyParts(home));
            otherDbm = this.app.getDbMapping(key.getStorageName());
        } else if (rel.createOnDemand()) {
            // a key for a virtually defined object that's never actually  stored in the db
            // or a key for an object that represents subobjects grouped by some property,
            // generated on the fly
            key = new SyntheticKey(home.getKey(), kstr);
        } else {
            // Not a relation we can use getNodeByRelation() for.
            return null;
        }

        // See if Transactor has already come across this node
        Node node = tx.getCleanNode(key);

        if (node != null && node.getState() != NodeInterface.INVALID) {
            // we used to refresh the node in the main cache here to avoid the primary key
            // entry being flushed from cache before the secondary one
            // (risking duplicate nodes in cache) but we don't need to since we fetched
            // the node from the threadlocal transactor cache and didn't refresh it in the
            // main cache.
            return node;
        }

        // try to get the node from the shared cache
        node = (Node) this.cache.get(key);

        // check if we can use the cached node without further checks.
        // we need further checks for subnodes fetched by name if the subnodes were changed.
        if (node != null && node.getState() != NodeInterface.INVALID) {
            // check if node is null node (cached null)
            if (node.isNullNode()) {
                // do not check reference nodes against child collection
                if (rel.isComplexReference() || node.created != home.getLastSubnodeChange()) {
                    node = null; //  cached null not valid anymore
                }
            } else if (!rel.virtual) {
                // apply different consistency checks for groupby nodes and database nodes:
                // for group nodes, check if they're contained
                if (rel.groupby != null) {
                    if (home.contains(node) < 0) {
                        node = null;
                    }

                // for database nodes, check if constraints are fulfilled
                } else if (!rel.usesPrimaryKey()) {
                    if (!rel.checkConstraints(home, node)) {
                        node = null;
                    }
                }
            }
        }

        if (node == null || node.getState() == NodeInterface.INVALID) {
            // The requested node isn't in the shared cache.
            // Synchronize with key to make sure only one version is fetched
            // from the database.
            node = getNodeByRelation(tx.txn, home, kstr, rel, otherDbm);

            if (node != null && node.getState() != NodeInterface.DELETED) {
                Node newNode = node;
                if (key.equals(node.getKey())) {
                    node = registerNewNode(node, null);
                } else {
                    node = registerNewNode(node, key);
                }
                // reset create time of old node, otherwise Relation.checkConstraints
                // will reject it under certain circumstances.
                if (node != newNode) {
                    node.created = node.lastmodified;
                }
            } else {
                // node fetched from db is null, cache result using nullNode
                synchronized (this.cache) {
                    // do not use child collection timestamp as cache guard for object references
                    long lastchange = rel.isComplexReference() ? 0 : home.getLastSubnodeChange();
                    this.cache.put(key, new Node(lastchange));

                    // we ignore the case that onother thread has created the node in the meantime
                    return null;
                }
            }
        } else if (node.isNullNode()) {
            // the nullNode caches a null value, i.e. an object that doesn't exist
            return null;
        } else {
            // update primary key in cache to keep it from being flushed, see above
            if (!rel.usesPrimaryKey() && node.getState() != NodeInterface.TRANSIENT) {
                synchronized (this.cache) {
                    Node old = (Node) this.cache.put(node.getKey(), node);

                    if (old != node && old != null && !old.isNullNode() &&
                            old.getState() != NodeInterface.INVALID) {
                        this.cache.put(node.getKey(), old);
                        this.cache.put(key, old);
                        node = old;
                    }
                }
            }
        }

        if (node != null) {
            tx.visitCleanNode(key, node);
        }

        return node;
    }

    /**
     * Register a newly created node in the node cache unless it it is already contained.
     * If so, the previously registered node is kept and returned. Otherwise, the onInit()
     * function is called on the new node and it is returned.
     * @param node the node to register
     * @return the newly registered node, or the one that was already registered with the node's key
     */
    private Node registerNewNode(Node node, KeyInterface secondaryKey) {
        KeyInterface key = node.getKey();
        RequestEvaluator reval = this.app.getCurrentRequestEvaluator();
        // if no request evaluator is associated with current thread, do not cache node
        // as we cannot invoke onInit() on it.
        if (reval == null) {
            Node old = (Node) this.cache.get(key);
            if (old != null && !old.isNullNode() && old.getState() != NodeInterface.INVALID) {
                return old;
            }
            return node;
        }

        synchronized(this.cache) {
            Node old = (Node) this.cache.put(key, node);

            if (old != null && !old.isNullNode() && old.getState() != NodeInterface.INVALID) {
                this.cache.put(key, old);
                if (secondaryKey != null) {
                    this.cache.put(secondaryKey, old);
                }
                return old;
            } else if (secondaryKey != null) {
                this.cache.put(secondaryKey, node);
            }
        }
        // New node is going ot be used, invoke onInit() on it
        // Invoke onInit() if it is defined by this Node's prototype
        try {
            // We need to reach deap into helma.framework.core to invoke onInit(),
            // but the functionality is really worth it.
            reval.invokeDirectFunction(node, "onInit", RequestEvaluator.EMPTY_ARGS); //$NON-NLS-1$
        } catch (Exception x) {
            this.app.logError(Messages.getString("NodeManager.0"), x); //$NON-NLS-1$
        }
        return node;
    }

    /**
     * Register a node in the node cache.
     */
    public void registerNode(Node node) {
        this.cache.put(node.getKey(), node);
    }

    /**
     * Register a node in the node cache using the key argument.
     */
    protected void registerNode(Node node, KeyInterface key) {
        this.cache.put(key, node);
    }

    /**
     * Remove a node from the node cache. If at a later time it is accessed again,
     * it will be refetched from the database.
     */
    public void evictNode(Node node) {
        node.setState(NodeInterface.INVALID);
        this.cache.remove(node.getKey());
    }

    /**
     * Remove a node from the node cache. If at a later time it is accessed again,
     * it will be refetched from the database.
     */
    public void evictNodeByKey(KeyInterface key) {
        Node n = (Node) this.cache.remove(key);

        if (n != null) {
            n.setState(NodeInterface.INVALID);

            if (!(key instanceof DbKey)) {
                this.cache.remove(n.getKey());
            }
        }
    }

    /**
     * Used when a key stops being valid for a node. The cached node itself
     * remains valid, if it is present in the cache by other keys.
     */
    public void evictKey(KeyInterface key) {
        this.cache.remove(key);
        // also drop key from thread-local transactor cache
        Transactor tx = Transactor.getInstance();
        if (tx != null) {
            tx.dropCleanNode(key);
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // methods to do the actual db work
    ////////////////////////////////////////////////////////////////////////

    /**
     *  Insert a new node in the embedded database or a relational database table,
     *  depending on its db mapping.
     *
     * @throws IOException
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    public void insertNode(DatabaseInterface db, TransactionInterface txn, Node node)
                    throws IOException, SQLException, NoDriverException {
        invokeOnPersist(node);
        DbMapping dbm = node.getDbMapping();

        if ((dbm == null) || !dbm.isRelational()) {
            db.insertNode(txn, node.getID(), node);
        } else {
            insertRelationalNode(node, dbm, dbm.getConnection());
        }
    }

    /**
     *  Insert a node into a different (relational) database than its default one.
     *
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    public void exportNode(Node node, DbSource dbs)
                    throws SQLException, NoDriverException {
        if (node == null) {
            throw new IllegalArgumentException(Messages.getString("NodeManager.1")); //$NON-NLS-1$
        }

        DbMapping dbm = node.getDbMapping();

        if (dbs == null) {
            throw new IllegalArgumentException(Messages.getString("NodeManager.2")); //$NON-NLS-1$
        } else if ((dbm == null) || !dbm.isRelational()) {
            throw new IllegalArgumentException(Messages.getString("NodeManager.3")); //$NON-NLS-1$
        } else {
            insertRelationalNode(node, dbm, dbs.getConnection());
        }
    }

    /**
     *  Insert a node into a different (relational) database than its default one.
     *
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    public void exportNode(Node node, DbMapping dbm)
                    throws SQLException, NoDriverException {
        if (node == null) {
            throw new IllegalArgumentException(Messages.getString("NodeManager.4")); //$NON-NLS-1$
        }

        if (dbm == null) {
            throw new IllegalArgumentException(Messages.getString("NodeManager.5")); //$NON-NLS-1$
        } else if (!dbm.isRelational()) {
            throw new IllegalArgumentException(Messages.getString("NodeManager.6")); //$NON-NLS-1$
        } else {
            insertRelationalNode(node, dbm, dbm.getConnection());
        }
    }

    /**
     * Insert a node into a relational database.
     *
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     * @throws SQLException
     */
    protected void insertRelationalNode(Node node, DbMapping dbm, Connection con)
                throws NoDriverException, SQLException {

        if (con == null) {
            throw new NullPointerException(Messages.getString("NodeManager.7")); //$NON-NLS-1$
        }

        // set connection to write mode
        if (con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(false);

        String insertString = dbm.getInsert();
        PreparedStatement stmt = con.prepareStatement(insertString);

        // app.logEvent ("inserting relational node: " + node.getID ());
        DbColumn[] columns = dbm.getColumns();

        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;

        try {
            int columnNumber = 1;

            for (int i = 0; i < columns.length; i++) {
                DbColumn col = columns[i];
                if (!col.isMapped())
                    continue;
                if (col.isIdField()) {
                    setStatementValue(stmt, columnNumber, node.getID(), col);
                } else if (col.isPrototypeField()) {
                    setStatementValue(stmt, columnNumber, dbm.getExtensionId(), col);
                } else {
                    Relation rel = col.getRelation();
                    Property p = rel == null ? null : node.getProperty(rel.getPropName());

                    // skip readonly relations
                    if (rel.readonly) {
                        continue;
                    }

                    if (p != null) {
                        setStatementValue(stmt, columnNumber, p, col.getType());
                    } else if (col.isNameField()) {
                        stmt.setString(columnNumber, node.getName());
                    } else {
                        stmt.setNull(columnNumber, col.getType());
                    }
                }
                columnNumber += 1;
            }
            stmt.executeUpdate();

        } finally {
            if (this.logSql) {
                long logTimeStop = java.lang.System.currentTimeMillis();
                logSqlStatement("SQL INSERT", dbm.getTableName(), //$NON-NLS-1$
                                logTimeStart, logTimeStop, insertString);
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {}
            }
        }
    }

    /**
     *  calls onPersist function for the HopObject
     */
    private void invokeOnPersist(Node node) {
        try {
            // We need to reach deap into helma.framework.core to invoke onPersist(),
            // but the functionality is really worth it.
            RequestEvaluator reval = this.app.getCurrentRequestEvaluator();
            if (reval != null) {
                reval.invokeDirectFunction(node, "onPersist", RequestEvaluator.EMPTY_ARGS); //$NON-NLS-1$
            }
        } catch (Exception x) {
            this.app.logError(Messages.getString("NodeManager.8"), x); //$NON-NLS-1$
        }
    }

    /**
     *  Updates a modified node in the embedded db or an external relational database, depending
     * on its database mapping.
     *
     * @return true if the DbMapping of the updated Node is to be marked as updated via
     *              DbMapping.setLastDataChange
     *
     * @throws IOException
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    public boolean updateNode(DatabaseInterface db, TransactionInterface txn, Node node)
                    throws IOException, SQLException, NoDriverException {

        invokeOnPersist(node);
        DbMapping dbm = node.getDbMapping();
        boolean markMappingAsUpdated = false;

        if ((dbm == null) || !dbm.isRelational()) {
            db.updateNode(txn, node.getID(), node);
        } else {
            Hashtable propMap = node.getPropMap();
            Property[] props;

            if (propMap == null) {
                props = new Property[0];
            } else {
                props = new Property[propMap.size()];
                propMap.values().toArray(props);
            }

            // make sure table meta info is loaded by dbmapping
            dbm.getColumns();

            StringBuffer b = dbm.getUpdate();

            // comma flag set after the first dirty column, also tells as
            // if there are dirty columns at all
            boolean comma = false;

            for (int i = 0; i < props.length; i++) {
                // skip clean properties
                if ((props[i] == null) || !props[i].dirty) {
                    // null out clean property so we don't consider it later
                    props[i] = null;
                    continue;
                }

                Relation rel = dbm.propertyToRelation(props[i].getName());

                // skip readonly, virtual and collection relations
                if ((rel == null) || rel.readonly || rel.virtual ||
                        (!rel.isPrimitiveOrReference())) {
                    // null out property so we don't consider it later
                    props[i] = null;
                    continue;
                }

                if (comma) {
                    b.append(", "); //$NON-NLS-1$
                } else {
                    comma = true;
                }

                b.append(rel.getDbField());
                b.append(" = ?"); //$NON-NLS-1$
            }

            // if no columns were updated, return false
            if (!comma) {
                return false;
            }

            b.append(" WHERE "); //$NON-NLS-1$
            dbm.appendCondition(b, dbm.getIDField(), node.getID(), true);

            Connection con = dbm.getConnection();
            // set connection to write mode
            if (con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(false);
            PreparedStatement stmt = con.prepareStatement(b.toString());

            int stmtNumber = 0;
            long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;

            try {
                for (int i = 0; i < props.length; i++) {
                    Property p = props[i];

                    if (p == null) {
                        continue;
                    }

                    Relation rel = dbm.propertyToRelation(p.getName());

                    stmtNumber++;
                    setStatementValue(stmt, stmtNumber, p, rel.getColumnType());

                    p.dirty = false;

                    if (!rel.isPrivate()) {
                        markMappingAsUpdated = true;
                    }
                }

                stmt.executeUpdate();

            } finally {
                if (this.logSql) {
                    long logTimeStop = System.currentTimeMillis();
                    logSqlStatement("SQL UPDATE", dbm.getTableName(), //$NON-NLS-1$
                                    logTimeStart, logTimeStop, b.toString());
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }

        // update may cause changes in the node's parent subnode array
        // TODO: is this really needed anymore?
        if (markMappingAsUpdated && node.isAnonymous()) {
            Node parent = node.getCachedParent();

            if (parent != null) {
                parent.markSubnodesChanged();
            }
        }

        return markMappingAsUpdated;
    }

    /**
     *  Performs the actual deletion of a node from either the embedded or an external
     *  SQL database.
     */
    public void deleteNode(DatabaseInterface db, TransactionInterface txn, Node node)
                    throws Exception {
        DbMapping dbm = node.getDbMapping();

        if ((dbm == null) || !dbm.isRelational()) {
            db.deleteNode(txn, node.getID());
        } else {
            Statement st = null;
            long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;
            String str = new StringBuffer("DELETE FROM ").append(dbm.getTableName()) //$NON-NLS-1$
                                                         .append(" WHERE ") //$NON-NLS-1$
                                                         .append(dbm.getIDField())
                                                         .append(" = ") //$NON-NLS-1$
                                                         .append(node.getID())
                                                         .toString();

            try {
                Connection con = dbm.getConnection();
                // set connection to write mode
                if (con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(false);

                st = con.createStatement();

                st.executeUpdate(str);

            } finally {
                if (this.logSql) {
                    long logTimeStop = System.currentTimeMillis();
                    logSqlStatement("SQL DELETE", dbm.getTableName(), //$NON-NLS-1$
                                    logTimeStart, logTimeStop, str);
                }
                if (st != null) {
                    try {
                        st.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }

        // node may still be cached via non-primary keys. mark as invalid
        node.setState(NodeInterface.INVALID);
    }


    /**
     * Generate a new ID for a given type, delegating to our IDGeneratorInterface if set.
     */
    public String generateID(DbMapping map) throws Exception {
        if (this.idgen != null) {
            // use our custom IDGeneratorInterface
            return this.idgen.generateID(map);
        }
        return doGenerateID(map);
    }

    /**
     * Actually generates an ID, using a method matching the given DbMapping.
     */
    public String doGenerateID(DbMapping map) throws Exception {
        if ((map == null) || !map.isRelational()) {
            // use embedded db id generator
            return generateEmbeddedID();
        }
        String idMethod = map.getIDgen();
        if (idMethod == null || "[max]".equalsIgnoreCase(idMethod) || map.isMySQL()) { //$NON-NLS-1$
            // use select max as id generator
            return generateMaxID(map);
        } else if ("[hop]".equalsIgnoreCase(idMethod)) { //$NON-NLS-1$
            // use embedded db id generator
            return generateEmbeddedID();
        } else {
            // use db sequence as id generator
            return generateSequenceID(map);
        }
    }

    /**
     * Gererates an ID for use with the embedded database.
     */
    synchronized String generateEmbeddedID() throws Exception {
        return this.db.nextID();
    }

    /**
     * Generates an ID for the table by finding out the maximum current value
     */
    synchronized String generateMaxID(DbMapping map)
                                      throws Exception {
        String retval = null;
        Statement stmt = null;
        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;
        String q = new StringBuffer("SELECT MAX(").append(map.getIDField()) //$NON-NLS-1$
                                                  .append(") FROM ") //$NON-NLS-1$
                                                  .append(map.getTableName())
                                                  .toString();

        try {
            Connection con = map.getConnection();
            // set connection to read-only mode
            if (!con.isReadOnly() && !map.isSQLite()) con.setReadOnly(true);

            stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(q);

            // check for empty table
            if (!rs.next()) {
                long currMax = map.getNewID(0);

                retval = Long.toString(currMax);
            } else {
                long currMax = rs.getLong(1);

                currMax = map.getNewID(currMax);
                retval = Long.toString(currMax);
            }
        } finally {
            if (this.logSql) {
                long logTimeStop = System.currentTimeMillis();
                logSqlStatement("SQL SELECT_MAX", map.getInsertUpdateTableName(), //$NON-NLS-1$
                                logTimeStart, logTimeStop, q);
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                }
            }
        }

        return retval;
    }

    String generateSequenceID(DbMapping map) throws Exception {
        Statement stmt = null;
        String retval = null;
        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;
        String q;
        if (map.isOracle()) {
            q = new StringBuffer("SELECT ").append(map.getIDgen()) //$NON-NLS-1$
                    .append(".nextval FROM dual").toString(); //$NON-NLS-1$
        } else if (map.isPostgreSQL() || map.isH2()) {
            q = new StringBuffer("SELECT nextval('") //$NON-NLS-1$
                    .append(map.getIDgen()).append("')").toString(); //$NON-NLS-1$
        } else {
            throw new RuntimeException(Messages.getString("NodeManager.9")); //$NON-NLS-1$
        }

        try {
            Connection con = map.getConnection();
            // TODO is it necessary to set connection to write mode here?
            if (con.isReadOnly() && !map.isSQLite()) con.setReadOnly(false);

            stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(q);

            if (!rs.next()) {
                throw new SQLException(Messages.getString("NodeManager.10")); //$NON-NLS-1$
            }

            retval = rs.getString(1);
        } finally {
            if (this.logSql) {
                long logTimeStop = System.currentTimeMillis();
                logSqlStatement("SQL SELECT_NEXTVAL", map.getTableName(), //$NON-NLS-1$
                                logTimeStart, logTimeStop, q);
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                }
            }
        }

        return retval;
    }

    /**
     *  Loades subnodes via subnode relation. Only the ID index is loaded, the nodes are
     *  loaded later on demand.
     */
    public List getNodeIDs(Node home, Relation rel) throws Exception {
        DbMapping type = rel == null ? null : rel.otherType;
        if (type == null || !type.isRelational()) {
            // this should never be called for embedded nodes
            throw new RuntimeException(Messages.getString("NodeManager.11") + home); //$NON-NLS-1$
        }
        List retval = new ArrayList();

        // if we do a groupby query (creating an intermediate layer of groupby nodes),
        // retrieve the value of that field instead of the primary key
        Connection con = type.getConnection();
        // set connection to read-only mode
        if (!con.isReadOnly() && !type.isSQLite()) con.setReadOnly(true);

        Statement stmt = null;
        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;
        String query = null;

        try {
            StringBuffer b = rel.getIdSelect();

            if (home.getSubnodeRelation() != null) {
                // subnode relation was explicitly set
                query = b.append(" ").append(home.getSubnodeRelation()).toString(); //$NON-NLS-1$
            } else {
                // let relation object build the query
                rel.buildQuery(b, home, true, false);
                query = b.toString();
            }

            stmt = con.createStatement();

            if (rel.maxSize > 0) {
                stmt.setMaxRows(rel.maxSize);
            }

            ResultSet result = stmt.executeQuery(query);

            // problem: how do we derive a SyntheticKey from a not-yet-persistent Node?
            KeyInterface k = (rel.groupby != null) ? home.getKey() : null;

            while (result.next()) {
                String kstr = result.getString(1);

                // jump over null values - this can happen especially when the selected
                // column is a group-by column.
                if (kstr == null) {
                    continue;
                }

                // make the proper key for the object, either a generic DB key or a groupby key
                KeyInterface key = (rel.groupby == null)
                        ? (KeyInterface) new DbKey(rel.otherType, kstr)
                        : (KeyInterface) new SyntheticKey(k, kstr);
                retval.add(new NodeHandle(key));

                // if these are groupby nodes, evict nullNode keys
                if (rel.groupby != null) {
                    Node n = (Node) this.cache.get(key);

                    if ((n != null) && n.isNullNode()) {
                        evictKey(key);
                    }
                }
            }
        } finally {
            if (this.logSql) {
                long logTimeStop = System.currentTimeMillis();
                logSqlStatement("SQL SELECT_IDS", type.getTableName(), //$NON-NLS-1$
                        logTimeStart, logTimeStop, query);
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                }
            }
        }

        return retval;
    }

    /**
     *  Loades subnodes via subnode relation. This is similar to getNodeIDs, but it
     *  actually loades all nodes in one go, which is better for small node collections.
     *  This method is used when xxx.loadmode=aggressive is specified.
     */
    public List getNodes(Node home, Relation rel) throws Exception {
        // This does not apply for groupby nodes - use getNodeIDs instead
        assert rel.groupby == null;

        if ((rel.otherType == null) || !rel.otherType.isRelational()) {
            // this should never be called for embedded nodes
            throw new RuntimeException(Messages.getString("NodeManager.12") + //$NON-NLS-1$
                                       home);
        }

        List retval = new ArrayList();
        DbMapping dbm = rel.otherType;

        Connection con = dbm.getConnection();
        // set connection to read-only mode
        if (!con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(true);

        Statement stmt = con.createStatement();
        DbColumn[] columns = dbm.getColumns();
        Relation[] joins = dbm.getJoins();
        String query = null;
        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;

        try {
            StringBuffer b = dbm.getSelect(rel);

            if (home.getSubnodeRelation() != null) {
                b.append(home.getSubnodeRelation());
            } else {
                // let relation object build the query
                rel.buildQuery(b, home, true, false);
            }

            query = b.toString();

            if (rel.maxSize > 0) {
                stmt.setMaxRows(rel.maxSize);
            }

            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                // re-get and re-cache the columns from the resultset
                columns = dbm.getColumns(rs);

                // create new Nodes.
                Node node = createNode(rel.otherType, rs, columns, 0);
                if (node == null) {
                    continue;
                }
                KeyInterface primKey = node.getKey();

                retval.add(new NodeHandle(primKey));

                registerNewNode(node, null);

                fetchJoinedNodes(rs, joins, columns.length);
            }

        } finally {
            if (this.logSql) {
                long logTimeStop = System.currentTimeMillis();
                logSqlStatement("SQL SELECT_ALL", dbm.getTableName(), //$NON-NLS-1$
                        logTimeStart, logTimeStop, query);
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                }
            }
        }

        return retval;
    }

    protected List collectMissingKeys(SubnodeList list, int start, int length) {
        List retval = null;
        for (int i = start; i < start + length; i++) {
            NodeHandle handle = list.get(i);
            if (handle != null && !this.cache.containsKey(handle.getKey())) {
                if (retval == null) {
                    retval = new ArrayList();
                }
                retval.add(handle.getKey().getID());
            }
        }
        return retval;
    }

    /**
     *
     */
    public void prefetchNodes(Node home, Relation rel, SubnodeList list, int start, int length)
                       throws Exception {
        DbMapping dbm = rel.otherType;

        // this does nothing for objects in the embedded database
        if (dbm != null && dbm.isRelational()) {
            // int missing = cache.containsKeys(keys);
            List missing = collectMissingKeys(list, start, length);

            if (missing != null) {
                Connection con = dbm.getConnection();
                // set connection to read-only mode
                if (!con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(true);

                Statement stmt = con.createStatement();
                DbColumn[] columns = dbm.getColumns();
                Relation[] joins = dbm.getJoins();
                String query = null;
                long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;

                try {
                    StringBuffer b = dbm.getSelect(null).append(" WHERE "); //$NON-NLS-1$
                    String idfield = (rel.groupby != null) ? rel.groupby : dbm.getIDField();
                    String[] ids = (String[]) missing.toArray(new String[missing.size()]);

                    dbm.appendCondition(b, idfield, ids);
                    dbm.addJoinConstraints(b, " AND "); //$NON-NLS-1$

                    if (rel.groupby != null) {
                        rel.renderConstraints(b, home, " AND "); //$NON-NLS-1$

                        if (rel.order != null) {
                            b.append(" ORDER BY "); //$NON-NLS-1$
                            b.append(rel.order);
                        }
                    }

                    query = b.toString();

                    ResultSet rs = stmt.executeQuery(query);

                    String groupbyProp = null;
                    HashMap groupbySubnodes = null;

                    if (rel.groupby != null) {
                        groupbyProp = dbm.columnNameToProperty(rel.groupby);
                        groupbySubnodes = new HashMap();
                    }

                    String accessProp = null;

                    if ((rel.accessName != null) && !rel.usesPrimaryKey()) {
                        accessProp = dbm.columnNameToProperty(rel.accessName);
                    }

                    while (rs.next()) {
                        // re-get and re-cache the columns from the resultset
                        columns = dbm.getColumns(rs);

                        // create new Nodes.
                        Node node = createNode(dbm, rs, columns, 0);
                        if (node == null) {
                            continue;
                        }
                        KeyInterface key = node.getKey();
                        KeyInterface secondaryKey = null;

                        // for grouped nodes, collect subnode lists for the intermediary
                        // group nodes.
                        String groupName = null;

                        if (groupbyProp != null) {
                            groupName = node.getString(groupbyProp);
                            if (groupName != null) {
                                Node groupNode = (Node) groupbySubnodes.get(groupName);

                                if (groupNode == null) {
                                    groupNode = home.getGroupbySubnode(groupName, true);
                                    groupbySubnodes.put(groupName, groupNode);
                                }

                                SubnodeList subnodes = groupNode.getSubnodeList();
                                if (subnodes == null) {
                                    subnodes = groupNode.createSubnodeList();
                                    // mark subnodes as up-to-date
                                    subnodes.lastSubnodeFetch = subnodes.getLastSubnodeChange();
                                }
                                subnodes.add(new NodeHandle(key));
                            }
                        }

                        // if relation doesn't use primary key as accessName, get secondary key
                        if (accessProp != null) {
                            String accessName = node.getString(accessProp);
                            if (accessName != null) {
                                if (groupName == null) {
                                    secondaryKey = new SyntheticKey(home.getKey(), accessName);
                                } else {
                                    KeyInterface groupKey = new SyntheticKey(home.getKey(), groupName);
                                    secondaryKey = new SyntheticKey(groupKey, accessName);
                                }
                            }

                        }

                        // register new nodes with the cache. If an up-to-date copy
                        // existed in the cache, use that.
                        registerNewNode(node, secondaryKey);
                        fetchJoinedNodes(rs, joins, columns.length);
                    }

                } catch (Exception x) {
                    this.app.logError(Messages.getString("NodeManager.13"), x); //$NON-NLS-1$
                } finally {
                    if (this.logSql) {
                        long logTimeStop = System.currentTimeMillis();
                        logSqlStatement("SQL SELECT_PREFETCH", dbm.getTableName(), //$NON-NLS-1$
                                        logTimeStart, logTimeStop, query);
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (Exception ignore) {
                        }
                    }
                }
            }
        }
    }

    /**
     * Count the nodes contained in the child collection of the home node
     * which is defined by Relation rel.
     */
    public int countNodes(Node home, Relation rel) throws Exception {
        DbMapping type = rel == null ? null : rel.otherType;
        if (type == null || !type.isRelational()) {
            // this should never be called for embedded nodes
            throw new RuntimeException(Messages.getString("NodeManager.14") + home); //$NON-NLS-1$
        }
        int retval = 0;
        Connection con = type.getConnection();
        // set connection to read-only mode
        if (!con.isReadOnly() && !type.isSQLite()) con.setReadOnly(true);

        Statement stmt = null;
        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;
        String query = null;

        try {
            StringBuffer b = rel.getCountSelect();

            if (home.getSubnodeRelation() != null) {
                // use the manually set subnoderelation of the home node
                query = b.append(" ").append(home.getSubnodeRelation()).toString(); //$NON-NLS-1$
            } else {
                // let relation object build the query
                rel.buildQuery(b, home, false, true);
                query = b.toString();
            }

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            if (!rs.next()) {
                retval = 0;
            } else {
                retval = rs.getInt(1);
            }
        } finally {
            if (this.logSql) {
                long logTimeStop = System.currentTimeMillis();
                logSqlStatement("SQL SELECT_COUNT", type.getTableName(), //$NON-NLS-1$
                        logTimeStart, logTimeStop, query);
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                }
            }
        }

        return (rel.maxSize > 0) ? Math.min(rel.maxSize, retval) : retval;
    }

    /**
     *  Similar to getNodeIDs, but returns a List that contains the nodes property names instead of IDs
     */
    public Vector getPropertyNames(Node home, Relation rel)
                            throws Exception {
        DbMapping type = rel == null ? null : rel.otherType;
        if (type == null || !type.isRelational()) {
            // this should never be called for embedded nodes
            throw new RuntimeException(Messages.getString("NodeManager.15") + home); //$NON-NLS-1$
        }
        Vector retval = new Vector();

        Connection con = rel.otherType.getConnection();
        // set connection to read-only mode
        if (!con.isReadOnly() && !type.isSQLite()) con.setReadOnly(true);

        Statement stmt = null;
        long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;
        String query = null;

        try {
            // NOTE: we explicitly convert tables StringBuffer to a String
            // before appending to be compatible with JDK 1.3
            StringBuffer b = rel.getNamesSelect();

            if (home.getSubnodeRelation() != null) {
                b.append(" ").append(home.getSubnodeRelation()); //$NON-NLS-1$
            } else {
                // let relation object build the query
                rel.buildQuery(b, home, true, false);
            }

            stmt = con.createStatement();

            query = b.toString();

            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String n = rs.getString(1);

                if (n != null) {
                    retval.add(n);
                }
            }
        } finally {
            if (this.logSql) {
                long logTimeStop = System.currentTimeMillis();
                logSqlStatement("SQL SELECT_ACCESSNAMES", type.getTableName(), //$NON-NLS-1$
                        logTimeStart, logTimeStop, query);
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                }
            }
        }

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    // private getNode methods
    ///////////////////////////////////////////////////////////////////////////////////////
    private Node getNodeByKey(TransactionInterface txn, DbKey key)
                       throws Exception {
        // Note: Key must be a DbKey, otherwise will not work for relational objects
        Node node = null;
        DbMapping dbm = this.app.getDbMapping(key.getStorageName());
        String kstr = key.getID();

        if ((dbm == null) || !dbm.isRelational()) {
            node = (Node) this.db.getNode(txn, kstr);
            if ((node != null) && (dbm != null)) {
                node.setDbMapping(dbm);
            }
        } else {
            String idfield = dbm.getIDField();

            Statement stmt = null;
            String query = null;
            long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;

            try {
                Connection con = dbm.getConnection();
                // set connection to read-only mode
                if (!con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(true);

                stmt = con.createStatement();

                DbColumn[] columns = dbm.getColumns();
                Relation[] joins = dbm.getJoins();

                StringBuffer b = dbm.getSelect(null).append("WHERE "); //$NON-NLS-1$
                dbm.appendCondition(b, idfield, kstr);
                dbm.addJoinConstraints(b, " AND "); //$NON-NLS-1$
                query = b.toString();

                ResultSet rs = stmt.executeQuery(query);

                if (!rs.next()) {
                    return null;
                }

                // re-get and re-cache the columns from the resultset
                columns = dbm.getColumns(rs);

                node = createNode(dbm, rs, columns, 0);

                fetchJoinedNodes(rs, joins, columns.length);

                if (rs.next()) {
                    this.app.logError(Messages.getString("NodeManager.16") + query); //$NON-NLS-1$
                }
            } finally {
                if (this.logSql) {
                    long logTimeStop = System.currentTimeMillis();
                    logSqlStatement("SQL SELECT_BYKEY", dbm.getTableName(), //$NON-NLS-1$
                                    logTimeStart, logTimeStop, query);
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ignore) {
                        // ignore
                    }
                }
            }
        }

        return node;
    }

    private Node getNodeByRelation(TransactionInterface txn, Node home, String kstr, Relation rel, DbMapping dbm)
                            throws Exception {
        Node node = null;

        if (rel != null && rel.virtual) {
            if (rel.needsPersistence()) {
                node = (Node) home.createNode(kstr);
            } else {
                node = new Node(home, kstr, this.safe, rel.prototype);
            }

            // set prototype and dbmapping on the newly created virtual/collection node
            node.setPrototype(rel.prototype);
            node.setDbMapping(rel.getVirtualMapping());
        } else if (rel != null && rel.groupby != null) {
            node = home.getGroupbySubnode(kstr, false);

            if (node == null && (dbm == null || !dbm.isRelational())) {
                node = (Node) this.db.getNode(txn, kstr);
            }
            return node;
        } else if (rel == null || dbm == null || !dbm.isRelational()) {
            node = (Node) this.db.getNode(txn, kstr);
            node.setDbMapping(dbm);
            return node;
        } else {
            Statement stmt = null;
            String query = null;
            long logTimeStart = this.logSql ? System.currentTimeMillis() : 0;

            try {
                Connection con = dbm.getConnection();
                // set connection to read-only mode
                if (!con.isReadOnly() && !dbm.isSQLite()) con.setReadOnly(true);
                Relation[] joins = dbm.getJoins();
                StringBuffer b = dbm.getSelect(rel);

                if (home.getSubnodeRelation() != null && !rel.isComplexReference()) {
                    // combine our key with the constraints in the manually set subnode relation
                    b.append(" WHERE "); //$NON-NLS-1$
                    dbm.appendCondition(b, rel.accessName, kstr);
                    // add join contraints in case this is an old oracle style join
                    dbm.addJoinConstraints(b, " AND "); //$NON-NLS-1$
                    // add potential constraints from manually set subnodeRelation
                    String subrel = home.getSubnodeRelation().trim();
                    if (subrel.length() > 5) {
                        b.append(" AND ("); //$NON-NLS-1$
                        b.append(subrel.substring(5).trim());
                        b.append(")"); //$NON-NLS-1$
                    }
                } else {
                    rel.buildQuery(b, home, dbm, kstr, false, false);
                }

                stmt = con.createStatement();

                query = b.toString();

                ResultSet rs = stmt.executeQuery(query);

                if (!rs.next()) {
                    return null;
                }

                // get and re-cache the columns from the current result set
                DbColumn[] columns = dbm.getColumns(rs);

                node = createNode(dbm, rs, columns, 0);

                fetchJoinedNodes(rs, joins, columns.length);

                if (rs.next()) {
                    this.app.logError(Messages.getString("NodeManager.17") + query); //$NON-NLS-1$
                }

            } finally {
                if (this.logSql) {
                    long logTimeStop = System.currentTimeMillis();
                    logSqlStatement("SQL SELECT_BYRELATION", dbm.getTableName(), //$NON-NLS-1$
                                    logTimeStart, logTimeStop, query);
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ignore) {
                        // ignore
                    }
                }
            }
        }

        return node;
    }

    /**
     *  Create a new Node from a ResultSet.
     *
     * @throws SQLException
     * @throws IOException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    public Node createNode(DbMapping dbm, ResultSet rs, DbColumn[] columns, int offset)
                throws SQLException, IOException, NoDriverException {
        HashMap propBuffer = new HashMap();
        String id = null;
        String name = null;
        String protoName = dbm.getTypeName();
        DbMapping dbmap = dbm;

        Node node = new Node(this.safe);

        for (int i = 0; i < columns.length; i++) {

            int columnNumber = i + 1 + offset;

            // set prototype?
            if (columns[i].isPrototypeField()) {
                String protoId = rs.getString(columnNumber);
                protoName = dbm.getPrototypeName(protoId);

                if (protoName != null) {
                    dbmap = getDbMapping(protoName);

                    if (dbmap == null) {
                        // invalid prototype name!
                        this.app.logError(Messages.getString("NodeManager.18") //$NON-NLS-1$
                                + protoName + Messages.getString("NodeManager.19") //$NON-NLS-1$
                                + dbm.getTypeName() + Messages.getString("NodeManager.20")); //$NON-NLS-1$
                        dbmap = dbm;
                        protoName = dbmap.getTypeName();
                    }
                }
            }

            // set id?
            if (columns[i].isIdField()) {
                id = rs.getString(columnNumber);
                // if id == null, the object doesn't actually exist - return null
                if (id == null) {
                    return null;
                }
            }

            // set name?
            if (columns[i].isNameField()) {
                name = rs.getString(columnNumber);
            }

            Property newprop = new Property(node);

            switch (columns[i].getType()) {
                case Types.BIT:
                case Types.BOOLEAN:
                    newprop.setBooleanValue(rs.getBoolean(columnNumber));

                    break;

                case Types.TINYINT:
                case Types.BIGINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    newprop.setIntegerValue(rs.getLong(columnNumber));

                    break;

                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                    newprop.setFloatValue(rs.getDouble(columnNumber));

                    break;

                case Types.DECIMAL:
                case Types.NUMERIC:

                    BigDecimal num = rs.getBigDecimal(columnNumber);
                    if (num == null) {
                        break;
                    }
                    if (num.scale() > 0) {
                        newprop.setFloatValue(num.doubleValue());
                    } else {
                        newprop.setIntegerValue(num.longValue());
                    }

                    break;

                case Types.VARBINARY:
                case Types.BINARY:
                    newprop.setJavaObjectValue(rs.getBytes(columnNumber));

                    break;

                case Types.BLOB:
                case Types.LONGVARBINARY:
                    {
                        InputStream in = rs.getBinaryStream(columnNumber);
                        if (in == null) {
                            break;
                        }
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        byte[] buffer = new byte[2048];
                        int read;
                        while ((read = in.read(buffer)) > -1) {
                            bout.write(buffer, 0, read);
                        }
                        newprop.setJavaObjectValue(bout.toByteArray());
                    }

                    break;

                case Types.LONGVARCHAR:
                    try {
                        newprop.setStringValue(rs.getString(columnNumber));
                    } catch (SQLException x) {
                        Reader in = rs.getCharacterStream(columnNumber);
                        if (in == null) {
                            newprop.setStringValue(null);
                            break;
                        }
                        StringBuffer out = new StringBuffer();
                        char[] buffer = new char[2048];
                        int read;
                        while ((read = in.read(buffer)) > -1) {
                            out.append(buffer, 0, read);
                        }
                        newprop.setStringValue(out.toString());
                    }

                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.OTHER:
                    newprop.setStringValue(rs.getString(columnNumber));

                    break;

                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    newprop.setDateValue(rs.getTimestamp(columnNumber));

                    break;

                case Types.NULL:
                    newprop.setStringValue(null);

                    break;

                case Types.CLOB:
                    Clob cl = rs.getClob(columnNumber);
                    if (cl == null) {
                        newprop.setStringValue(null);
                        break;
                    }
                    char[] c = new char[(int) cl.length()];
                    Reader isr = cl.getCharacterStream();
                    isr.read(c);
                    newprop.setStringValue(String.copyValueOf(c));
                    break;

                default:
                    newprop.setStringValue(rs.getString(columnNumber));

                    break;
            }

            if (rs.wasNull()) {
                newprop.setStringValue(null);
            }

            propBuffer.put(columns[i].getName(), newprop);

            // mark property as clean, since it's fresh from the db
            newprop.dirty = false;
        }

        if (id == null) {
            return null;
        }
        Transactor tx = Transactor.getInstance();
        if (tx != null) {
            // Check if the node is already registered with the transactor -
            // it may be in the process of being DELETED, but do return the
            // new node if the old one has been marked as INVALID.
            DbKey key = new DbKey(dbmap, id);
            Node dirtyNode = tx.getDirtyNode(key);
            if (dirtyNode != null && dirtyNode.getState() != NodeInterface.INVALID) {
                return dirtyNode;
            }
        }

        Hashtable propMap = new Hashtable();
        DbColumn[] columns2 = dbmap.getColumns();
        for (int i=0; i<columns2.length; i++) {
            Relation rel = columns2[i].getRelation();
            if (rel != null && rel.isPrimitiveOrReference()) {
                Property prop = (Property) propBuffer.get(columns2[i].getName());

                if (prop == null) {
                    continue;
                }

                prop.setName(rel.propName);

                // if the property is a pointer to another node, change the property type to NODE
                if (rel.isReference() && rel.usesPrimaryKey()) {
                    // FIXME: References to anything other than the primary key are not supported
                    prop.convertToNodeReference(rel);
                }
                propMap.put(rel.propName, prop);
            }
        }

        node.init(dbmap, id, name, protoName, propMap);
        return node;
    }

    /**
     *  Fetch nodes that are fetched additionally to another node via join.
     *
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     * @throws SQLException
     * @throws IOException
     */
    private void fetchJoinedNodes(ResultSet rs, Relation[] joins, int offset)
            throws NoDriverException, SQLException, IOException {
        int resultSetOffset = offset;
        // create joined objects
        for (int i = 0; i < joins.length; i++) {
            DbMapping jdbm = joins[i].otherType;
            // get and re-cache the columns from the resultset
            DbColumn[] columns = jdbm.getColumns(rs);

            Node node = createNode(jdbm, rs, columns, resultSetOffset);
            if (node != null) {
                registerNewNode(node, null);
            }
            resultSetOffset += columns.length;
        }
    }


    /**
     * Get a DbMapping for a given prototype name. This is just a proxy
     * method to the app's getDbMapping() method.
     */
    public DbMapping getDbMapping(String protoname) {
        return this.app.getDbMapping(protoname);
    }

    /**
     *  Get an array of the the keys currently held in the object cache
     */
    public Object[] getCacheEntries() {
        return this.cache.getCachedObjects();
    }

    /**
     * Get the number of elements in the object cache
     */
    public int countCacheEntries() {
        return this.cache.size();
    }

    /**
     * Clear the object cache, causing all objects to be recreated.
     */
    public void clearCache() {
        synchronized (this.cache) {
            this.cache.clear();
        }
    }

    /**
     * Returns a map with statistics about the cache
     */
    public Map getCacheStatistics() {
        return cache.getStatistics();
    }

    /**
     * Add a listener that is notified each time a transaction commits
     * that adds, modifies or deletes any Nodes.
     */
    public void addNodeChangeListener(NodeChangeListenerInterface listener) {
        this.listeners.add(listener);
    }

    /**
     * Remove a previously added NodeChangeListenerInterface.
     */
    public void removeNodeChangeListener(NodeChangeListenerInterface listener) {
        this.listeners.remove(listener);
    }

    /**
     * Let transactors know if they should collect and fire NodeChangeListenerInterface
     * events
     */
    protected boolean hasNodeChangeListeners() {
        return this.listeners.size() > 0;
    }

    /**
     * Called by transactors after committing.
     */
    protected void fireNodeChangeEvent(List inserted, List updated, List deleted, List parents) {
        int l = this.listeners.size();

        for (int i=0; i<l; i++) {
            try {
                ((NodeChangeListenerInterface) this.listeners.get(i)).nodesChanged(inserted, updated, deleted, parents);
            } catch (Error e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void setStatementValue(PreparedStatement stmt, int columnNumber, String value, DbColumn col)
            throws SQLException {
        if (value == null) {
            stmt.setNull(columnNumber, col.getType());
        } else if (col.needsQuotes()) {
            stmt.setString(columnNumber, value);
        } else {
            stmt.setLong(columnNumber, Long.parseLong(value));
        }
    }

    private void setStatementValue(PreparedStatement stmt, int stmtNumber, Property p, int columnType)
            throws SQLException {
        if (p.getValue() == null) {
            stmt.setNull(stmtNumber, columnType);
        } else {
            switch (columnType) {
                case Types.BIT:
                case Types.BOOLEAN:
                    stmt.setBoolean(stmtNumber, p.getBooleanValue());

                    break;

                case Types.TINYINT:
                case Types.BIGINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    stmt.setLong(stmtNumber, p.getIntegerValue());

                    break;

                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    stmt.setDouble(stmtNumber, p.getFloatValue());

                    break;

                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                case Types.BINARY:
                case Types.BLOB:
                    Object b = p.getJavaObjectValue();
                    if (b instanceof byte[]) {
                        byte[] buf = (byte[]) b;
                        try {
                            stmt.setBytes(stmtNumber, buf);
                        } catch (SQLException x) {
                            ByteArrayInputStream bout = new ByteArrayInputStream(buf);
                            stmt.setBinaryStream(stmtNumber, bout, buf.length);
                        }
                    } else {
                        throw new SQLException(Messages.getString("NodeManager.21") + //$NON-NLS-1$
                                p.getName() + Messages.getString("NodeManager.22") + b.getClass()); //$NON-NLS-1$
                    }

                    break;

                case Types.LONGVARCHAR:
                    try {
                        stmt.setString(stmtNumber, p.getStringValue());
                    } catch (SQLException x) {
                        String str = p.getStringValue();
                        Reader r = new StringReader(str);
                        stmt.setCharacterStream(stmtNumber, r, str.length());
                    }

                    break;

                case Types.CLOB:
                    String val = p.getStringValue();
                    Reader isr = new StringReader (val);
                    stmt.setCharacterStream (stmtNumber,isr, val.length());

                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.OTHER:
                    stmt.setString(stmtNumber, p.getStringValue());

                    break;

                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    stmt.setTimestamp(stmtNumber, p.getTimestampValue());

                    break;

                case Types.NULL:
                    stmt.setNull(stmtNumber, 0);

                    break;

                default:
                    stmt.setString(stmtNumber, p.getStringValue());

                    break;
            }
        }
    }

    private void logSqlStatement(String type, String table,
                                 long logTimeStart, long logTimeStop, String statement) {
        // init sql-log if necessary
        if (this.sqlLog == null) {
            String sqlLogName = this.app.getProperty("sqlLog", "helma."+this.app.getName()+".sql");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
            this.sqlLog = LogFactory.getLog(sqlLogName);
        }

        this.sqlLog.info(new StringBuffer().append(type)
                                      .append(" ") //$NON-NLS-1$
                                      .append(table)
                                      .append(" ") //$NON-NLS-1$
                                      .append((logTimeStop - logTimeStart))
                                      .append(": ") //$NON-NLS-1$
                                      .append(statement)
                                      .toString());
    }
}
