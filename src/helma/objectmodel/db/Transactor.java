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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import helma.framework.core.RequestEvaluator;
import helma.objectmodel.DatabaseException;
import helma.objectmodel.NodeInterface;
import helma.objectmodel.TransactionInterface;
import helma.scripting.ScriptingEngineInterface;
import helma.scripting.ScriptingException;

/**
 * A subclass of thread that keeps track of changed nodes and triggers
 * changes in the database when a transaction is commited.
 */
public class Transactor {

    // The associated node manager
    NodeManager nmgr;

    // List of nodes to be updated
    private Map dirtyNodes;

    // List of visited clean nodes
    private Map cleanNodes;

    // List of nodes whose child index has been modified
    private Set parentNodes;

    // Is a transaction in progress?
    private volatile boolean active;
    private volatile boolean killed;

    // Transaction for the embedded database
    protected TransactionInterface txn;

    // Transactions for SQL data sources
    private Map<DbSource, Connection> sqlConnections;

    // Set of SQL connections that already have been verified
    private Map<DbSource, Long> testedConnections;

    // when did the current transaction start?
    private long tstart;

    // a name to log the transaction. For HTTP transactions this is the rerquest path
    private String tname;

    // the thread we're associated with
    private Thread thread;

    private ArrayList<Transaction> transactions = new ArrayList<Transaction>();

    private static final ThreadLocal txtor = new ThreadLocal();

    /**
     * Creates a new Transactor object.
     *
     * @param nmgr the NodeManager used to fetch and persist nodes.
     */
    private Transactor(NodeManager nmgr) {
        this.thread = Thread.currentThread();
        this.nmgr = nmgr;

        this.dirtyNodes = new LinkedHashMap();
        this.cleanNodes = new HashMap();
        this.parentNodes = new HashSet();

        this.sqlConnections = new HashMap();
        this.testedConnections = new HashMap();
        this.active = false;
        this.killed = false;
    }

    /**
     * Get the transactor for the current thread or null if none exists.
     * @return the transactor associated with the current thread
     */
    public static Transactor getInstance() {
        return (Transactor) txtor.get();
    }

    /**
     * Get the transactor for the current thread or throw a IllegalStateException if none exists.
     * @return the transactor associated with the current thread
     * @throws IllegalStateException if no transactor is associated with the current thread
     */
    public static Transactor getInstanceOrFail() throws IllegalStateException {
        Transactor tx = (Transactor) txtor.get();
        if (tx == null)
            throw new IllegalStateException(Messages.getString("Transactor.0") + //$NON-NLS-1$
                Messages.getString("Transactor.1")); //$NON-NLS-1$
        return tx;
    }

    /**
     * Get the transactor for the current thread, creating a new one if none exists.
     * @param nmgr the NodeManager used to create the transactor
     * @return the transactor associated with the current thread
     */
    public static Transactor getInstance(NodeManager nmgr) {
        Transactor t = (Transactor) txtor.get();
        if (t == null) {
            t = new Transactor(nmgr);
            txtor.set(t);
        }
        return t;
    }

    /**
     * Mark a Node as modified/created/deleted during this transaction
     *
     * @param node ...
     */
    public void visitDirtyNode(Node node) {
        if (node != null) {
            KeyInterface key = node.getKey();

            if (node.getState() == NodeInterface.DELETED && this.dirtyNodes.containsKey(key)) {
            	// remove a known deleted node (will be re-added at the end of the list),
            	// because it might not have been deleted yet when we were last modified
            	// about it being dirty, which could result in a on commit removal order
            	// which does not equal the removal order as done in the request's
            	// application code
            	this.dirtyNodes.remove(key);
            }

            this.dirtyNodes.put(key, node);
        }
    }

    /**
     * Unmark a Node that has previously been marked as modified during the transaction
     *
     * @param node ...
     */
    public void dropDirtyNode(Node node) {
        if (node != null) {
            KeyInterface key = node.getKey();

            this.dirtyNodes.remove(key);
        }
    }

    /**
     * Get a dirty Node from this transaction.
     * @param key the key
     * @return the dirty node associated with the key, or null
     */
    public Node getDirtyNode(KeyInterface key) {
        return (Node) this.dirtyNodes.get(key);
    }

    /**
     * Keep a reference to an unmodified Node local to this transaction
     *
     * @param node the node to register
     */
    public void visitCleanNode(Node node) {
        if (node != null) {
            KeyInterface key = node.getKey();

            if (!this.cleanNodes.containsKey(key)) {
                this.cleanNodes.put(key, node);
            }
        }
    }

    /**
     * Keep a reference to an unmodified Node local to this transaction
     *
     * @param key the key to register with
     * @param node the node to register
     */
    public void visitCleanNode(KeyInterface key, Node node) {
        if (node != null) {
            if (!this.cleanNodes.containsKey(key)) {
                this.cleanNodes.put(key, node);
            }
        }
    }

    /**
     * Drop a reference to an unmodified Node previously registered with visitCleanNode().
     * @param key the key
     */
    public void dropCleanNode(KeyInterface key) {
        this.cleanNodes.remove(key);
    }

    /**
     * Get a reference to an unmodified Node local to this transaction
     *
     * @param key ...
     *
     * @return ...
     */
    public Node getCleanNode(Object key) {
        return (key == null) ? null : (Node) this.cleanNodes.get(key);
    }

    /**
     *
     *
     * @param node ...
     */
    public void visitParentNode(Node node) {
        this.parentNodes.add(node);
    }


    /**
     * Returns true if a transaction is currently active.
     * @return true if currently a transaction is active
     */
    public boolean isActive() {
        return this.active;
    }

    /**
     * Check whether the thread associated with this transactor is alive.
     * This is a proxy to Thread.isAlive().
     * @return true if the thread running this transactor is currently alive.
     */
    public boolean isAlive() {
        return this.thread != null && this.thread.isAlive();
    }

    /**
     * Register a db connection with this transactor thread.
     * @param src the db source
     * @param con the connection
     */
    public void registerConnection(DbSource src, Connection con) {
        this.sqlConnections.put(src, con);
        // we assume a freshly created connection is ok.
        this.testedConnections.put(src, new Long(System.currentTimeMillis()));
    }

    /**
     * Get a db connection that was previously registered with this transactor thread.
     * @param src the db source
     * @return the connection
     */
    public Connection getConnection(DbSource src) {
        Connection con = (Connection) this.sqlConnections.get(src);
        Long tested = (Long) this.testedConnections.get(src);
        long now = System.currentTimeMillis();
        if (con != null && (tested == null || now - tested.longValue() > 60000)) {
            // Check if the connection is still alive by executing a simple statement.
            try {
                Statement stmt = con.createStatement();
                stmt.execute("SELECT 1"); //$NON-NLS-1$
                stmt.close();
                this.testedConnections.put(src, new Long(now));
            } catch (SQLException sx) {
                try {
                    con.close();
                } catch (SQLException ignore) {/* nothing to do */}
                return null;
            }
        }
        return con;
    }

    /**
     * Start a new transaction with the given name.
     *
     * @param name The name of the transaction. This is usually the request
     * path for the underlying HTTP request.
     *
     * @throws Exception ...
     */
    public synchronized void begin(String name) throws Exception {
        if (this.killed) {
            throw new DatabaseException(Messages.getString("Transactor.2")); //$NON-NLS-1$
        } else if (this.active) {
            abort();
        }

        this.dirtyNodes.clear();
        this.cleanNodes.clear();
        this.parentNodes.clear();
        this.txn = this.nmgr.db.beginTransaction();
        this.active = true;
        this.tstart = System.currentTimeMillis();
        this.tname = name;
    }

    /**
     * Commit the current transaction, persisting all changes to DB.
     *
     * @throws Exception ...
     */
    public synchronized void commit() throws Exception {

    	Iterator connections = this.sqlConnections.values().iterator();
        while (connections.hasNext()) {
        	Connection connection = (Connection) connections.next();
        	if (connection.getMetaData().supportsTransactions() && !connection.getAutoCommit()) {
        		connection.commit();
        	}
        }

        // the request evaluator's scripting engine
        ScriptingEngineInterface engine = null;
        // check if a request evaluator is associated with the current thread
        if (nmgr.app.getCurrentRequestEvaluator() != null) {
            // get the request evaluator's scripting engine
            engine = nmgr.app.getCurrentRequestEvaluator().getScriptingEngine();
        }

        // check if a scripting engine is available
        if (engine != null) {
            try {
                // call onBeforeCommit, if defined
                engine.invoke(null, "onBeforeCommit", RequestEvaluator.EMPTY_ARGS, //$NON-NLS-1$
                        ScriptingEngineInterface.ARGS_WRAP_DEFAULT, false);
            } catch (ScriptingException e) {
                nmgr.app.logError(Messages.getString("Transactor.17"), e); //$NON-NLS-1$
            }
        }

        int inserted = 0;
        int updated = 0;
        int deleted = 0;

        ArrayList insertedNodes = null;
        ArrayList updatedNodes = null;
        ArrayList deletedNodes = null;
        ArrayList modifiedParentNodes = null;
        // if nodemanager has listeners collect dirty nodes
        boolean hasListeners = nmgr.hasNodeChangeListeners();
        // check if a scripting engine is available and has a function called "onCommit"
        boolean hasOnCommit = engine != null && engine.hasFunction(null, "onCommit", false); //$NON-NLS-1$
        boolean collectNodes = hasListeners || hasOnCommit;

        if (collectNodes) {
            insertedNodes = new ArrayList();
            updatedNodes = new ArrayList();
            deletedNodes = new ArrayList();
            modifiedParentNodes = new ArrayList();
        }

        Transaction transaction = new Transaction();

        if (!this.dirtyNodes.isEmpty()) {
            Object[] dirty = this.dirtyNodes.values().toArray();

            // the set to collect DbMappings to be marked as changed
            HashSet dirtyDbMappings = new HashSet();

            for (int i = 0; i < dirty.length; i++) {
                Node node = (Node) dirty[i];

                // update nodes in db
                int nstate = node.getState();

                if (nstate == NodeInterface.NEW) {
                    this.nmgr.insertNode(this.nmgr.db, this.txn, node);
                    dirtyDbMappings.add(node.getDbMapping());
                    node.setState(NodeInterface.CLEAN);

                    // register node with nodemanager cache
                    nmgr.registerNode(node);

                    if (collectNodes) {
                        insertedNodes.add(node);
                    }

                    transaction.addInsertedNode(node);
                } else if (nstate == NodeInterface.MODIFIED) {
                    // only mark DbMapping as dirty if updateNode returns true
                    if (this.nmgr.updateNode(this.nmgr.db, this.txn, node)) {
                        dirtyDbMappings.add(node.getDbMapping());
                    }
                    node.setState(NodeInterface.CLEAN);

                    // update node with nodemanager cache
                    nmgr.registerNode(node);

                    if (collectNodes) {
                        updatedNodes.add(node);
                    }

                    transaction.addModifiedNode(node);
                } else if (nstate == NodeInterface.DELETED) {
                    this.nmgr.deleteNode(this.nmgr.db, this.txn, node);
                    dirtyDbMappings.add(node.getDbMapping());

                    // remove node from nodemanager cache
                    nmgr.evictNode(node);

                    if (collectNodes) {
                        deletedNodes.add(node);
                    }

                    transaction.addDeletedNode(node);
                }

                node.clearWriteLock();
            }

            // set last data change times in db-mappings
            for (Iterator i = dirtyDbMappings.iterator(); i.hasNext(); ) {
                DbMapping dbm = (DbMapping) i.next();
                if (dbm != null) {
                    dbm.setLastDataChange();
                }
            }
        }

        if (!this.parentNodes.isEmpty()) {
            // set last subnode change times in parent nodes
            for (Iterator i = this.parentNodes.iterator(); i.hasNext(); ) {
                Node node = (Node) i.next();
                node.markSubnodesChanged();
                if (collectNodes) {
                    modifiedParentNodes.add(node);
                }
            }
        }
        
        if (hasListeners) {
            nmgr.fireNodeChangeEvent(insertedNodes, updatedNodes,
                                     deletedNodes, modifiedParentNodes);
        }

        // clear the node collections
        recycle();

        if (active) {
            active = false;
            // call onCommit if defined, passing inserted, updated, deleted and
            // parents to it
            if (hasOnCommit) {
                try {
                    engine.invoke(null, "onCommit", new Object[] { //$NON-NLS-1$
                            insertedNodes.toArray(),
                            updatedNodes.toArray(),
                            deletedNodes.toArray(),
                            modifiedParentNodes.toArray()
                    }, ScriptingEngineInterface.ARGS_WRAP_DEFAULT, false);
                } catch (ScriptingException e) {
                    nmgr.app.logError(Messages.getString("Transactor.19"), e); //$NON-NLS-1$
                }
            }
            nmgr.db.commitTransaction(txn);
            
            // check if a scripting engine is available
            if (engine != null) {
                // call onAfterCommit, if defined
                try {
                    engine.invoke(null, Messages.getString("Transactor.20"), RequestEvaluator.EMPTY_ARGS, //$NON-NLS-1$
                            ScriptingEngineInterface.ARGS_WRAP_DEFAULT, false);
                } catch (ScriptingException e) {
                    nmgr.app.logError(Messages.getString("Transactor.18"), e); //$NON-NLS-1$
                }
            }
            
            txn = null;
        }

        StringBuffer msg = new StringBuffer(tname).append(Messages.getString("Transactor.21")) //$NON-NLS-1$
                .append(System.currentTimeMillis() - tstart).append(Messages.getString("Transactor.22")); //$NON-NLS-1$
        if(inserted + updated + deleted > 0) {
            msg.append(" [+") //$NON-NLS-1$
                    .append(inserted).append(", ~") //$NON-NLS-1$
                    .append(updated).append(", -") //$NON-NLS-1$
                    .append(deleted).append("]"); //$NON-NLS-1$
        }
        nmgr.app.logAccess(msg.toString());

        // unset transaction name
        tname = null;
    }

    /**
     * Abort the current transaction, rolling back all changes made.
     */
    public synchronized void abort() {
    	Iterator<Transaction> iterator = this.transactions.iterator();
    	while (iterator.hasNext()) {
    		Transaction transaction = iterator.next();

    		// evict dirty nodes from cache
    		Iterator<Node> dirtyNodes = transaction.getDirtyNodes().iterator();
    		while (dirtyNodes.hasNext()) {
    			Node node = dirtyNodes.next();

    			// Declare node as invalid, so it won't be used by other threads
                // that want to write on it and remove it from cache
    			this.nmgr.evictNode(node);
                node.clearWriteLock();
    		}

            // set last subnode change times in parent nodes
            Iterator<Node> updatedParentNodes = transaction.getUpdatedParentNodes().iterator();
            while (updatedParentNodes.hasNext()) {
            	Node node = updatedParentNodes.next();
                node.markSubnodesChanged();
            }
    	}

    	// clear the node collections
        recycle();
        this.transactions.clear();

        Iterator connections = this.sqlConnections.values().iterator();
        while (connections.hasNext()) {
        	Connection connection = (Connection) connections.next();
        	try {
				if (connection.getMetaData().supportsTransactions() && !connection.getAutoCommit()) {
					connection.rollback();
				}
			} catch (SQLException e) {
				this.nmgr.app.logError(Messages.getString("Transactor.12"), e); //$NON-NLS-1$
			}
        }

        // close any JDBC connections associated with this transactor thread
        closeConnections();

        if (this.active) {
            this.active = false;

            if (this.txn != null) {
                this.nmgr.db.abortTransaction(this.txn);
                this.txn = null;
            }

            this.nmgr.app.logAccess(this.tname + Messages.getString("Transactor.13") + //$NON-NLS-1$
                               (System.currentTimeMillis() - this.tstart) + Messages.getString("Transactor.14")); //$NON-NLS-1$
        }

        // unset transaction name
        this.tname = null;
    }

    /**
     * Kill this transaction thread. Used as last measure only.
     */
    @SuppressWarnings("deprecation")
    public synchronized void kill() {

        this.killed = true;
        this.thread.interrupt();

        // Interrupt the thread if it has not noticed the flag (e.g. because it is busy
        // reading from a network socket).
        if (this.thread.isAlive()) {
            this.thread.interrupt();
            try {
                this.thread.join(1000);
            } catch (InterruptedException ir) {
                // interrupted by other thread
            }
        }

        if (this.thread.isAlive() && "true".equals(this.nmgr.app.getProperty("requestTimeoutStop"))) { //$NON-NLS-1$ //$NON-NLS-2$
            // still running - check if we ought to stop() it
            try {
                Thread.sleep(2000);
                if (this.thread.isAlive()) {
                    // thread is still running, pull emergency break
                    this.nmgr.app.logEvent(Messages.getString("Transactor.15") + this); //$NON-NLS-1$
                    this.thread.stop();
                }
            } catch (InterruptedException ir) {
                // interrupted by other thread
            }
        }
    }

    /**
     * Closes all open JDBC connections
     */
    public void closeConnections() {
        if (this.sqlConnections != null) {
            for (Iterator i = this.sqlConnections.values().iterator(); i.hasNext();) {
                try {
                    Connection con = (Connection) i.next();

                    con.close();
                    this.nmgr.app.logEvent(Messages.getString("Transactor.16") + con); //$NON-NLS-1$
                } catch (Exception ignore) {
                    // exception closing db connection, ignore
                }
            }

            this.sqlConnections.clear();
            this.testedConnections.clear();
        }
    }

    /**
     * Clear collections and throw them away. They may have grown large,
     * so the benefit of keeping them (less GC) needs to be weighted against
     * the potential increas in memory usage.
     */
    private synchronized void recycle() {
        // clear the node collections to ease garbage collection
        this.dirtyNodes.clear();
        this.cleanNodes.clear();
        this.parentNodes.clear();
    }

    /**
     * Return the name of the current transaction. This is usually the request
     * path for the underlying HTTP request.
     */
    public String getTransactionName() {
        return this.tname;
    }

    /**
     * Return a string representation of this Transactor thread
     *
     * @return ...
     */
    @Override
    public String toString() {
        return "Transactor[" + this.tname + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}
