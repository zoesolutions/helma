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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import helma.framework.core.Application;
import helma.framework.core.Prototype;
import helma.util.ResourceProperties;

/**
 * A DbMapping describes how a certain type of  Nodes is to mapped to a
 * relational database table. Basically it consists of a set of JavaScript property-to-
 * Database row bindings which are represented by instances of the Relation class.
 */
public final class DbMapping {
    // DbMappings belong to an application
    protected final Application app;

    // prototype name of this mapping
    private final String typename;

    // properties from where the mapping is read
    private final Properties props;

    // name of data dbSource to which this mapping writes
    private DbSource dbSource;

    // name of datasource
    private String dbSourceName;

    // name of db table
    private String tableName;
    
    // name of db table to use for insert and/or update
    private String insertUpdateTableName;

    // the verbatim, unparsed _parent specification
    private String parentSetting;

    // list of properties to try for parent
    private ParentInfo[] parentInfo;

    // Relations describing subnodes and properties.
    protected Relation subRelation;
    protected Relation propRelation;

    // if this defines a subnode mapping with groupby layer,
    // we need a DbMapping for those groupby nodes
    private DbMapping groupbyMapping;

    // Map of property names to Relations objects
    private HashMap prop2db;

    // Map of db columns to Relations objects.
    // Case insensitive, keys are stored in lower case so
    // lookups must do a toLowerCase().
    private HashMap db2prop;

    // list of columns to fetch from db
    private DbColumn[] columns = null;

    // Map of db columns by name
    private HashMap columnMap;

    // Array of aggressively loaded references
    private Relation[] joins;

    // pre-rendered select statement
    private String selectString = null;
    private String insertString = null;
    private String updateString = null;

    // db field used as primary key
    private String idField;

    // db field used as object name
    private String nameField;

    // db field used to identify name of prototype to use for object instantiation
    private String protoField;

    // Used to map prototype ids to prototype names for
    // prototypes which extend the prototype represented by
    // this DbMapping.
    private ResourceProperties extensionMap;

    // a numeric or literal id used to represent this type in db
    private String extensionId;

    // dbmapping of parent prototype, if any
    private DbMapping parentMapping;

    // descriptor for key generation method
    private String idgen;

    // remember last key generated for this table
    private long lastID;

    // timestamp of last modification of the mapping (type.properties)
    // init value is -1 so we know we have to run update once even if
    // the underlying properties file is non-existent
    long lastTypeChange = -1;

    // timestamp of last modification of an object of this type
    long lastDataChange = 0;

    // Set of mappings that depend on us and should be forwarded last data change events
    HashSet dependentMappings = new HashSet();

    // does this DbMapping describe a virtual node (collection, mountpoint, groupnode)?
    private boolean isVirtual = false;

    // does this Dbmapping describe a group node?
    private boolean isGroup = false;

    /**
     * Create an internal DbMapping used for "virtual" mappings aka collections, mountpoints etc.
     */
    public DbMapping(Application app, String parentTypeName) {
        this(app, parentTypeName, null);
        // DbMappings created with this constructor always define virtual nodes
        this.isVirtual = true;
        if (parentTypeName != null) {
            this.parentMapping = app.getDbMapping(parentTypeName);
            if (this.parentMapping == null) {
                throw new IllegalArgumentException(Messages.getString("DbMapping.0") + parentTypeName); //$NON-NLS-1$
            }
        }
    }

    /**
     * Create a DbMapping from a type.properties property file
     */
    public DbMapping(Application app, String typename, Properties props, boolean virtual) {
        this(app,  typename, props);
        this.isVirtual = virtual;
    }

    /**
     * Create a DbMapping from a type.properties property file
     */
    public DbMapping(Application app, String typename, Properties props) {
        this.app = app;
        // create a unique instance of the string. This is useful so
        // we can compare types just by using == instead of equals.
        this.typename = typename == null ? null : typename.intern();

        this.prop2db = new HashMap();
        this.db2prop = new HashMap();
        this.columnMap = new HashMap();
        this.parentInfo = null;
        this.idField = null;
        this.props = props;

        if (props != null) {
            readBasicProperties();
        }
    }

    /**
     * Tell the type manager whether we need update() to be called
     */
    public boolean needsUpdate() {
        if (this.props instanceof ResourceProperties) {
            return ((ResourceProperties) this.props).lastModified() != this.lastTypeChange;
        }
        return false;
    }

    /**
     * Read in basic properties and register dbmapping with the
     * dbsource.
     */
    private void readBasicProperties() {
        this.tableName = this.props.getProperty("_table"); //$NON-NLS-1$
        // get the name of the table to use for insert/update operations, defaulting to the table to use 
        // for select operations, i.e. defaulting to use the same (the default) table for all operations
        this.insertUpdateTableName = this.props.getProperty("_tableInsertUpdate", this.tableName); //$NON-NLS-1$
        this.dbSourceName = this.props.getProperty("_db"); //$NON-NLS-1$

        if (this.dbSourceName != null) {
            this.dbSource = this.app.getDbSource(this.dbSourceName);

            if (this.dbSource == null) {
                this.app.logError(Messages.getString("DbMapping.1") + this.typename + //$NON-NLS-1$
                             Messages.getString("DbMapping.2") + this.dbSourceName); //$NON-NLS-1$
                this.app.logError(Messages.getString("DbMapping.3") + this.typename + //$NON-NLS-1$
                             Messages.getString("DbMapping.4")); //$NON-NLS-1$
            } else if (this.tableName == null) {
                this.app.logError(Messages.getString("DbMapping.5") + this.typename); //$NON-NLS-1$
                this.app.logError(Messages.getString("DbMapping.6") + this.typename + //$NON-NLS-1$
                             Messages.getString("DbMapping.7")); //$NON-NLS-1$

                // mark mapping as invalid by nulling the dbSource field
                this.dbSource = null;
            } else {
                // dbSource and tableName not null - register this instance
                this.dbSource.registerDbMapping(this);
            }
        }
    }



    /**
     * Read the mapping from the Properties. Return true if the properties were changed.
     * The read is split in two, this method and the rewire method. The reason is that in order
     * for rewire to work, all other db mappings must have been initialized and registered.
     */
    public synchronized void update() {
        // read in properties
        readBasicProperties();
        this.idgen = this.props.getProperty("_idgen"); //$NON-NLS-1$
        // if id field is null, we assume "ID" as default. We don't set it
        // however, so that if null we check the parent prototype first.
        this.idField = this.props.getProperty("_id"); //$NON-NLS-1$
        this.nameField = this.props.getProperty("_name"); //$NON-NLS-1$
        this.protoField = this.props.getProperty("_prototype"); //$NON-NLS-1$

        this.parentSetting = this.props.getProperty("_parent"); //$NON-NLS-1$
        if (this.parentSetting != null) {
            // comma-separated list of properties to be used as parent
            StringTokenizer st = new StringTokenizer(this.parentSetting, ",;"); //$NON-NLS-1$
            this.parentInfo = new ParentInfo[st.countTokens()];

            for (int i = 0; i < this.parentInfo.length; i++) {
                this.parentInfo[i] = new ParentInfo(st.nextToken().trim());
            }
        } else {
            this.parentInfo = null;
        }

        this.lastTypeChange = this.props instanceof ResourceProperties ?
                ((ResourceProperties) this.props).lastModified() : System.currentTimeMillis();

        // see if this prototype extends (inherits from) any other prototype
        String extendsProto = this.props.getProperty("_extends"); //$NON-NLS-1$

        if (extendsProto != null) {
            this.parentMapping = this.app.getDbMapping(extendsProto);
            if (this.parentMapping == null) {
                this.app.logError(Messages.getString("DbMapping.8") + this.typename + //$NON-NLS-1$
                             Messages.getString("DbMapping.9") + extendsProto); //$NON-NLS-1$
            } else {
                if (this.parentMapping.needsUpdate()) {
                    this.parentMapping.update();
                }
                // if tableName or DbSource are inherited from the parent mapping
                // set them to null so we are aware of the fact.
                if (this.tableName != null &&
                        this.tableName.equals(this.parentMapping.getTableName())) {
                    this.tableName = null;
                }
                // if insertUpdateTableName or DbSource are inherited from the parent mapping
                // set them to null so we are aware of the fact.
                if (this.insertUpdateTableName != null &&
                        this.insertUpdateTableName.equals(this.parentMapping.getInsertUpdateTableName())) {
                    this.insertUpdateTableName = null;
                }
                if (this.dbSourceName != null &&
                        this.dbSourceName.equals(this.parentMapping.getDbSourceName())) {
                    this.dbSourceName = null;
                    this.dbSource = null;
                }
            }
        } else {
            this.parentMapping = null;
        }

        if (inheritsStorage() && getPrototypeField() == null) {
            this.app.logError(Messages.getString("DbMapping.10") + this.typename); //$NON-NLS-1$
            this.app.logError(Messages.getString("DbMapping.11") + extendsProto); //$NON-NLS-1$
        }

        // check if there is an extension-id specified inside the type.properties
        this.extensionId = this.props.getProperty("_extensionId", this.typename); //$NON-NLS-1$
        registerExtension(this.extensionId, this.typename);

        // set the parent prototype in the corresponding Prototype object!
        // this was previously done by TypeManager, but we need to do it
        // ourself because DbMapping.update() may be called by other code than
        // the TypeManager.
        if (this.typename != null &&
                !"global".equalsIgnoreCase(this.typename) && //$NON-NLS-1$
                !"hopobject".equalsIgnoreCase(this.typename)) { //$NON-NLS-1$
            Prototype proto = this.app.getPrototypeByName(this.typename);
            if (proto != null) {
                if (extendsProto != null) {
                    proto.setParentPrototype(this.app.getPrototypeByName(extendsProto));
                } else if (!this.app.isJavaPrototype(this.typename)) {
                    proto.setParentPrototype(this.app.getPrototypeByName("hopobject")); //$NON-NLS-1$
                }
            }
        }

        // null the cached columns and select string
        this.columns = null;
        this.columnMap.clear();
        this.selectString = this.insertString = this.updateString = null;

        HashMap p2d = new HashMap();
        HashMap d2p = new HashMap();
        ArrayList joinList = new ArrayList();

        for (Iterator it = this.props.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry =  (Map.Entry) it.next();

            try {
                String propName = (String) entry.getKey();

                // ignore internal properties (starting with "_") and sub-options (containing a ".")
                if (!propName.startsWith("_") && propName.indexOf(".") < 0) { //$NON-NLS-1$ //$NON-NLS-2$
                    Object propValue = entry.getValue();
                    propName = app.correctPropertyName(propName);

                    // check if a relation for this propery already exists. If so, reuse it
                    Relation rel = (Relation) this.prop2db.get(propName);

                    if (rel == null) {
                        rel = new Relation(propName, this);
                    }

                    rel.update(propValue, getSubProperties(propName));
                    p2d.put(propName, rel);

                    if ((rel.columnName != null) && rel.isPrimitiveOrReference()) {
                        Relation old = (Relation) d2p.put(rel.columnName.toLowerCase(), rel);
                        // check if we're overwriting another relation
                        // if so, primitive relations get precendence to references
                        if (old != null) {
                            if (rel.isPrimitive() && old.isPrimitive()) {
                                this.app.logEvent(Messages.getString("DbMapping.12") + this.typename + "." + rel.columnName);  //$NON-NLS-1$//$NON-NLS-2$
                            } else if (rel.isReference() && old.isPrimitive()) {
                                // if a column is used both in a primitive and a reference mapping,
                                // use primitive mapping as primary one and mark reference as
                                // complex so it will be fetched separately
                                d2p.put(old.columnName.toLowerCase(), old);
                                rel.reftype = Relation.COMPLEX_REFERENCE;
                            } else if (rel.isPrimitive() && old.isReference()) {
                                old.reftype = Relation.COMPLEX_REFERENCE;
                            }
                        }
                    }

                    // check if a reference is aggressively fetched
                    if (rel.aggressiveLoading &&
                            (rel.isReference() || rel.isComplexReference())) {
                        joinList.add(rel);
                    }

                    // app.logEvent ("Mapping "+propName+" -> "+dbField);
                }
            } catch (Exception x) {
                this.app.logEvent(Messages.getString("DbMapping.13") + x.getMessage()); //$NON-NLS-1$
            }
        }

        this.prop2db = p2d;
        this.db2prop = d2p;

        this.joins = new Relation[joinList.size()];
        this.joins = (Relation[]) joinList.toArray(this.joins);

        Object subnodeMapping = this.props.get("_children"); //$NON-NLS-1$

        if (subnodeMapping != null) {
            try {
                // check if subnode relation already exists. If so, reuse it
                if (this.subRelation == null) {
                    this.subRelation = new Relation("_children", this); //$NON-NLS-1$
                }

                this.subRelation.update(subnodeMapping, getSubProperties("_children")); //$NON-NLS-1$

                // if subnodes are accessed via access name or group name,
                // the subnode relation is also the property relation.
                if ((this.subRelation.accessName != null) || (this.subRelation.groupby != null)) {
                    this.propRelation = this.subRelation;
                } else {
                    this.propRelation = null;
                }
            } catch (Exception x) {
                this.app.logEvent(Messages.getString("DbMapping.14") + this.typename + ": " +  //$NON-NLS-1$//$NON-NLS-2$
                             x.getMessage());

                // subRelation = null;
            }
        } else {
            this.subRelation = this.propRelation = null;
        }

        if (this.groupbyMapping != null) {
            initGroupbyMapping();
            this.groupbyMapping.lastTypeChange = this.lastTypeChange;
        }
    }

    /**
     * Add the given extensionId and the coresponding prototypename
     * to extensionMap for later lookup.
     * @param extID the id mapping to the prototypename recogniced by helma
     * @param extName the name of the extending prototype
     */
    private void registerExtension(String extID, String extName) {
        // lazy initialization of extensionMap
        if (extID == null) {
            return;
        }
        if (this.extensionMap == null) {
            this.extensionMap = new ResourceProperties();
            this.extensionMap.setIgnoreCase(true);
        } else if (this.extensionMap.containsValue(extName)) {
            // remove any preexisting mapping for the given childmapping
            this.extensionMap.values().remove(extName);
        }
        this.extensionMap.setProperty(extID, extName);
        if (inheritsStorage()) {
            this.parentMapping.registerExtension(extID, extName);
        }
    }

    /**
     * Returns the Set of Prototypes extending this prototype
     * @return the Set of Prototypes extending this prototype
     */
    public String[] getExtensions() {
        return this.extensionMap == null
                ? new String[] { this.extensionId }
                : (String[]) this.extensionMap.keySet().toArray(new String[0]);
    }

    /**
     * Looks up the prototype name identified by the given id, returing
     * our own type name if it can't be resolved
     * @param id the id specified for the prototype
     * @return the name of the extending prototype
     */
    public String getPrototypeName(String id) {
        if (inheritsStorage()) {
            return this.parentMapping.getPrototypeName(id);
        }
        // fallback to base-prototype if the proto isn't recogniced
        if (id == null) {
            return this.typename;
        }
        return this.extensionMap.getProperty(id, this.typename);
    }

    /**
     * get the id-value of this extension
     */
    public String getExtensionId() {
        return this.extensionId;
    }

    /**
     * Method in interface Updatable.
     */
    public void remove() {
        // do nothing, removing of type properties is not implemented.
    }

    /**
     * Get a JDBC connection for this DbMapping.
     *
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     * @throws SQLException
     */
    public Connection getConnection() throws NoDriverException, SQLException {
        if (this.dbSourceName == null) {
            if (this.parentMapping != null) {
                return this.parentMapping.getConnection();
            }
            throw new SQLException(Messages.getString("DbMapping.15")); //$NON-NLS-1$
        }

        if (this.tableName == null) {
            throw new SQLException(Messages.getString("DbMapping.16") + this); //$NON-NLS-1$
        }

        // if dbSource was previously not available, check again
        if (this.dbSource == null) {
            this.dbSource = this.app.getDbSource(this.dbSourceName);
        }

        if (this.dbSource == null) {
            throw new SQLException(Messages.getString("DbMapping.17") + this.dbSourceName + ".");  //$NON-NLS-1$//$NON-NLS-2$
        }

        return this.dbSource.getConnection();
    }

    /**
     * Get the DbSource object for this DbMapping. The DbSource describes a JDBC
     * data source including URL, JDBC driver, username and password.
     */
    public DbSource getDbSource() {
        if (this.dbSource == null) {
            if (this.dbSourceName != null) {
                this.dbSource = this.app.getDbSource(this.dbSourceName);
            } else if (this.parentMapping != null) {
                return this.parentMapping.getDbSource();
            }
        }

        return this.dbSource;
    }

    /**
     * Get the dbsource name used for this type mapping.
     */
    public String getDbSourceName() {
        if ((this.dbSourceName == null) && (this.parentMapping != null)) {
            return this.parentMapping.getDbSourceName();
        }

        return this.dbSourceName;
    }

    /**
     * Get the table name used for this type mapping.
     */
    public String getTableName() {
        if ((this.tableName == null) && (this.parentMapping != null)) {
            return this.parentMapping.getTableName();
        }

        return this.tableName;
    }
    
    /**
     * Returns the name of the table to use for insert/update operations.
     * 
     * @return The name of the table to use for insert/update operations.
     */
    public String getInsertUpdateTableName() {
        // check if no insert/update tale is defined, but a parent mapping, which might define one, exists
        if ((this.insertUpdateTableName == null) && (this.parentMapping != null)) {
            // return the insert/update table from the parent mapping
            return this.parentMapping.getInsertUpdateTableName();
        }

        // return the name of the table to use for insert/update operations
        return this.insertUpdateTableName;
    }

    /**
     * Get the application this DbMapping belongs to.
     */
    public Application getApplication() {
        return this.app;
    }

    /**
     * Get the name of this mapping's application
     */
    public String getAppName() {
        return this.app.getName();
    }

    /**
     * Get the name of the object type this DbMapping belongs to.
     */
    public String getTypeName() {
        return this.typename;
    }

    /**
     * Get the name of this type's parent type, if any.
     */
    public String getExtends() {
        return this.parentMapping == null ? null : this.parentMapping.getTypeName();
    }

    /**
     * Get the primary key column name for objects using this mapping.
     */
    public String getIDField() {
        if ((this.idField == null) && (this.parentMapping != null)) {
            return this.parentMapping.getIDField();
        }

        return (this.idField == null) ? "ID" : this.idField; //$NON-NLS-1$
    }

    /**
     * Get the column used for (internal) names of objects of this type.
     */
    public String getNameField() {
        if ((this.nameField == null) && (this.parentMapping != null)) {
            return this.parentMapping.getNameField();
        }

        return this.nameField;
    }

    /**
     * Get the column used for names of prototype.
     */
    public String getPrototypeField() {
        if ((this.protoField == null) && (this.parentMapping != null)) {
            return this.parentMapping.getPrototypeField();
        }

        return this.protoField;
    }

    /**
     * Translate a database column name to an object property name according to this mapping.
     */
    public String columnNameToProperty(String columnName) {
        if (columnName == null) {
            return null;
        }

        // SEMIHACK: If columnName is a function call, try to extract actual
        // column name from it
        int open = columnName.indexOf('(');
        int close = columnName.indexOf(')');
        if (open > -1 && close > open) {
            columnName = columnName.substring(open + 1, close);
        }

        return _columnNameToProperty(columnName.toLowerCase());
    }

    private String _columnNameToProperty(final String columnName) {
        Relation rel = (Relation) this.db2prop.get(columnName);

        if ((rel == null) && (this.parentMapping != null)) {
            return this.parentMapping._columnNameToProperty(columnName);
        }

        if ((rel != null) && rel.isPrimitiveOrReference()) {
            return rel.propName;
        }

        return null;
    }

    /**
     * Translate an object property name to a database column name according
     * to this mapping. If no mapping is found, the property name is returned,
     * assuming property and column names are equal.
     */
    public String propertyToColumnName(String propName) {
        if (propName == null) {
            return null;
        }

        return _propertyToColumnName(propName);
    }

    private String _propertyToColumnName(final String propName) {
        Relation rel = (Relation) this.prop2db.get(this.app.correctPropertyName(propName));

        if ((rel == null) && (this.parentMapping != null)) {
            return this.parentMapping._propertyToColumnName(propName);
        }

        if ((rel != null) && (rel.isPrimitiveOrReference())) {
            return rel.columnName;
        }

        return null;
    }

    /**
     * Translate a database column name to an object property name according to this mapping.
     */
    public Relation columnNameToRelation(String columnName) {
        if (columnName == null) {
            return null;
        }

        return _columnNameToRelation(columnName.toLowerCase());
    }

    private Relation _columnNameToRelation(final String columnName) {
        Relation rel = (Relation) this.db2prop.get(columnName);

        if ((rel == null) && (this.parentMapping != null)) {
            return this.parentMapping._columnNameToRelation(columnName);
        }

        return rel;
    }

    /**
     * Translate an object property name to a database column name according to this mapping.
     */
    public Relation propertyToRelation(String propName) {
        if (propName == null) {
            return null;
        }

        return _propertyToRelation(propName);
    }

    private Relation _propertyToRelation(String propName) {
        Relation rel = (Relation) this.prop2db.get(this.app.correctPropertyName(propName));

        if ((rel == null) && (this.parentMapping != null)) {
            return this.parentMapping._propertyToRelation(propName);
        }

        return rel;
    }

    /**
     * @return the parent info as unparsed string.
     */
    public String getParentSetting() {
        if ((this.parentSetting == null) && (this.parentMapping != null)) {
            return this.parentMapping.getParentSetting();
        }
        return this.parentSetting;
    }

    /**
     * @return the parent info array, which tells an object of this type how to
     * determine its parent object.
     */
    public synchronized ParentInfo[] getParentInfo() {
        if ((this.parentInfo == null) && (this.parentMapping != null)) {
            return this.parentMapping.getParentInfo();
        }

        return this.parentInfo;
    }

    /**
     *
     *
     * @return ...
     */
    public DbMapping getSubnodeMapping() {
        if (this.subRelation != null) {
            return this.subRelation.otherType;
        }

        if (this.parentMapping != null) {
            return this.parentMapping.getSubnodeMapping();
        }

        return null;
    }

    /**
     *
     *
     * @param propname ...
     *
     * @return ...
     */
    public DbMapping getPropertyMapping(String propname) {
        Relation rel = getPropertyRelation(propname);

        if (rel != null) {
            // if this is a virtual node, it doesn't have a dbmapping
            if (rel.virtual && (rel.prototype == null)) {
                return null;
            }
            return rel.otherType;
        }

        return null;
    }

    /**
     * If subnodes are grouped by one of their properties, return the
     * db-mapping with the right relations to create the group-by nodes
     */
    public synchronized DbMapping getGroupbyMapping() {
        if (this.subRelation == null && this.parentMapping != null) {
            return this.parentMapping.getGroupbyMapping();
        } else if (this.subRelation == null || this.subRelation.groupby == null) {
            return null;
        } else if (this.groupbyMapping == null) {
            initGroupbyMapping();
        }

        return this.groupbyMapping;
    }

    /**
     * Initialize the dbmapping used for group-by nodes.
     */
    private void initGroupbyMapping() {
        // if a prototype is defined for groupby nodes, use that
        // if mapping doesn' exist or isn't defined, create a new (anonymous internal) one
        this.groupbyMapping = new DbMapping(this.app, this.subRelation.groupbyPrototype);
        this.groupbyMapping.lastTypeChange = this.lastTypeChange;
        this.groupbyMapping.isGroup = true;

        // set subnode and property relations
        this.groupbyMapping.subRelation = this.subRelation.getGroupbySubnodeRelation();

        if (this.propRelation != null) {
            this.groupbyMapping.propRelation = this.propRelation.getGroupbyPropertyRelation();
        } else {
            this.groupbyMapping.propRelation = this.subRelation.getGroupbyPropertyRelation();
        }
    }

    /**
     *
     *
     * @param rel ...
     */
    public void setPropertyRelation(Relation rel) {
        this.propRelation = rel;
    }

    /**
     *
     *
     * @return ...
     */
    public Relation getSubnodeRelation() {
        if ((this.subRelation == null) && (this.parentMapping != null)) {
            return this.parentMapping.getSubnodeRelation();
        }

        return this.subRelation;
    }

    /**
     * Return the list of defined property names as String array.
     */
    public String[] getPropertyNames() {
        return (String[]) this.prop2db.keySet().toArray(new String[this.prop2db.size()]);
    }

    /**
     *
     *
     * @return ...
     */
    private Relation getPropertyRelation() {
        if ((this.propRelation == null) && (this.parentMapping != null)) {
            return this.parentMapping.getPropertyRelation();
        }

        return this.propRelation;
    }

    /**
     *
     *
     * @param propname ...
     *
     * @return ...
     */
    public Relation getPropertyRelation(String propname) {
        if (propname == null) {
            return getPropertyRelation();
        }

        // first try finding an exact match for the property name
        Relation rel = getExactPropertyRelation(propname);

        // if not defined, return the generic property mapping
        if (rel == null) {
            rel = getPropertyRelation();
        }

        return rel;
    }

    /**
     *
     *
     * @param propname ...
     *
     * @return ...
     */
    public Relation getExactPropertyRelation(String propname) {
        if (propname == null) {
            return null;
        }

        Relation rel = (Relation) this.prop2db.get(this.app.correctPropertyName(propname));

        if ((rel == null) && (this.parentMapping != null)) {
            rel = this.parentMapping.getExactPropertyRelation(propname);
        }

        return rel;
    }

    /**
     *
     *
     * @return ...
     */
    public String getSubnodeGroupby() {
        if ((this.subRelation == null) && (this.parentMapping != null)) {
            return this.parentMapping.getSubnodeGroupby();
        }

        return (this.subRelation == null) ? null : this.subRelation.groupby;
    }

    /**
     *
     *
     * @return ...
     */
    public String getIDgen() {
        if ((this.idgen == null) && (this.parentMapping != null)) {
            return this.parentMapping.getIDgen();
        }

        return this.idgen;
    }

    /**
     *
     *
     * @return ...
     */
    public WrappedNodeManager getWrappedNodeManager() {
        if (this.app == null) {
            throw new RuntimeException(Messages.getString("DbMapping.18")); //$NON-NLS-1$
        }

        return this.app.getWrappedNodeManager();
    }

    /**
     *  Tell whether this data mapping maps to a relational database table. This returns true
     *  if a datasource is specified, even if it is not a valid one. Otherwise, objects with invalid
     *  mappings would be stored in the embedded db instead of an error being thrown, which is
     *  not what we want.
     */
    public boolean isRelational() {
        return this.dbSourceName != null || (this.parentMapping != null && this.parentMapping.isRelational());
    }

    /**
     * Update and return the columns meta-information for this database mapping based on the given result-set's
     * meta-information.
     * New columns meta-information will be added, but missing columns meta-information will not be removed. Consecutive
     * calls can as such only extend the missing columns meta-information.
     *
     * Some JDBC drivers, especially JDBC drivers for NoSQL databases or other schema-less databases (e.g. LDAP) only
     * provide schema-meta-information together with result-sets. While this is true for some relational databases as
     * well, for relational databases it is easy to work around this limitation retrieving a fake result-set (e.g.
     * something like "SELECT * FROM Table WHERE 1 = 0"). Some JDBC drivers for the above mentioned non-relational
     * databases (e.g. LDAP) do however not even fully support SQL, so the generic workaround can not be used and
     * specific workaround for specific drivers would be needed, so rather than retrieving the schema-meta-information
     * from fake result-sets, the schema-meta-information is gradually updated and extended from real result-sets.
     *
     * @param resultSet
     *  The result-set to use for updating the columns meta-information.
     * @return
     *  The columns meta-information for this database mapping based on the given result-set's meta-information, but not
     *  the updated columns meta-information.
     *
     * @throws SQLException
     */
    public synchronized DbColumn[] getColumns(ResultSet resultSet) throws SQLException {
        // create a temporary hash table for all columns to avoid duplicates
        LinkedHashMap<String, DbColumn> allColumns = new LinkedHashMap<String, DbColumn>();

        // loop the existing columns (if there are existing columns)
        for (int i = 0; this.columns != null && i < this.columns.length; i++) {
            // put the current column to the linked hash map
            allColumns.put(this.columns[i].getName(), this.columns[i]);
        }

        // caching
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numberOfColumns = metaData.getColumnCount();

        // create a temporary array for the result set's columns
        DbColumn[] columns = new DbColumn[numberOfColumns];

        // flag indicating, if there are new columns
        boolean newColumns = false;
        // loop the result-set's columns
        for (int i = 0; i < numberOfColumns; i++) {
            // get the current column's column name
            String columnName = metaData.getColumnName(i + 1);
            // create the db column
            DbColumn dbColumn = new DbColumn(columnName, metaData.getColumnType(i + 1),
                    columnNameToRelation(columnName), this);
            // add the column to the result set's columns
            columns[i] = dbColumn;

            // check if the column is not known yet
            if (!allColumns.containsKey(columnName)) {
                // add the column to the linked hash map
                allColumns.put(columnName, dbColumn);
                // there are new columns
                newColumns = true;
            }
        }

        // overwrite the existing array
        this.columns = allColumns.values().toArray(new DbColumn[allColumns.size()]);

        // check if there are new columns
        if (newColumns) {
            // re-set the pre-rendered SQL statement
            this.selectString = null;
            this.insertString = null;
            this.updateString = null;
        }

        // return the result-set's columns
        return columns;
    }

    /**
     * Return an array of DbColumns for the relational table mapped by this DbMapping.
     *
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     * @throws SQLException
     */
    public synchronized DbColumn[] getColumns()
                                        throws NoDriverException, SQLException {
        // Use local variable cols to avoid synchronization (schema may be nulled elsewhere)
        if (this.columns == null) {
            // we do two things here: set the SQL type on the Relation mappings
            // and build a string of column names.
            Connection con = getConnection();
            Statement stmt = con.createStatement();
            String table = getTableName();

            if (table == null) {
                throw new SQLException(Messages.getString("DbMapping.20") + this); //$NON-NLS-1$
            }

            ResultSet rs = stmt.executeQuery(new StringBuffer("SELECT * FROM ").append(table) //$NON-NLS-1$
                                                                               .append(" WHERE 1 = 0") //$NON-NLS-1$
                                                                               .toString());

            if (rs == null) {
                throw new SQLException(Messages.getString("DbMapping.21") + this); //$NON-NLS-1$
            }

            this.getColumns(rs);
        }

        return this.columns;
    }

    /**
     *  Return the array of relations that are fetched with objects of this type.
     */
    public Relation[] getJoins() {
        return this.joins;
    }

    /**
     *
     *
     * @param columnName ...
     *
     * @return ...
     *
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     * @throws SQLException
     */
    public DbColumn getColumn(String columnName)
                       throws NoDriverException, SQLException {
        DbColumn col = (DbColumn) this.columnMap.get(columnName);
        if (col == null) {
            DbColumn[] cols = this.columns;
            if (cols == null) {
                cols = getColumns();
            }
            for (int i = 0; i < cols.length; i++) {
                if (columnName.equalsIgnoreCase(cols[i].getName())) {
                    col = cols[i];
                    break;
                }
            }
            this.columnMap.put(columnName, col);
        }
        return col;
    }

    /**
     *  Get a StringBuffer initialized to the first part of the select statement
     *  for objects defined by this DbMapping
     *
     * @param rel the Relation we use to select. Currently only used for optimizer hints.
     *            Is null if selecting by primary key.
     * @return the StringBuffer containing the first part of the select query
     */
    public StringBuffer getSelect(Relation rel) {
        // assign to local variable first so we are thread safe
        // (selectString may be reset by other threads)
        String sel = this.selectString;
        boolean isOracle = isOracle();

        if (rel == null && sel != null) {
            return new StringBuffer(sel);
        }

        StringBuffer s = new StringBuffer("SELECT "); //$NON-NLS-1$

        if (rel != null && rel.queryHints != null) {
            s.append(rel.queryHints).append(" "); //$NON-NLS-1$
        }

        String table = getTableName();

        // all columns from the main table
        s.append(table);
        s.append(".*"); //$NON-NLS-1$

        for (int i = 0; i < this.joins.length; i++) {
            if (!this.joins[i].otherType.isRelational()) {
                continue;
            }
            s.append(", "); //$NON-NLS-1$
            s.append(Relation.JOIN_PREFIX);
            s.append(this.joins[i].propName);
            s.append(".*"); //$NON-NLS-1$
        }

        s.append(" FROM "); //$NON-NLS-1$

        s.append(table);

        if (rel != null) {
            rel.appendAdditionalTables(s);
        }

        s.append(" "); //$NON-NLS-1$

        for (int i = 0; i < this.joins.length; i++) {
            if (!this.joins[i].otherType.isRelational()) {
                continue;
            }
            if (isOracle) {
                // generate an old-style oracle left join - see
                // http://www.praetoriate.com/oracle_tips_outer_joins.htm
                s.append(", "); //$NON-NLS-1$
                s.append(this.joins[i].otherType.getTableName());
                s.append(" "); //$NON-NLS-1$
                s.append(Relation.JOIN_PREFIX);
                s.append(this.joins[i].propName);
                s.append(" "); //$NON-NLS-1$
            } else {
                s.append("LEFT OUTER JOIN "); //$NON-NLS-1$
                s.append(this.joins[i].otherType.getTableName());
                s.append(" "); //$NON-NLS-1$
                s.append(Relation.JOIN_PREFIX);
                s.append(this.joins[i].propName);
                s.append(" ON "); //$NON-NLS-1$
                this.joins[i].renderJoinConstraints(s, isOracle);
            }
        }

        // cache rendered string for later calls, but only if it wasn't
        // built for a particular Relation
        if (rel == null) {
            this.selectString = s.toString();
        }

        return s;
    }

    /**
     *
     *
     * @return ...
     *
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     * @throws SQLException
     */
    public String getInsert() throws NoDriverException, SQLException {
        String ins = this.insertString;

        if (ins != null) {
            return ins;
        }

        StringBuffer b1 = new StringBuffer("INSERT INTO "); //$NON-NLS-1$
        StringBuffer b2 = new StringBuffer(" ) VALUES ( "); //$NON-NLS-1$
        // append the name of the table to use for insert/update operations
        b1.append(getInsertUpdateTableName());
        b1.append(" ( "); //$NON-NLS-1$

        DbColumn[] cols = getColumns();
        boolean needsComma = false;

        for (int i = 0; i < cols.length; i++) {
            if (cols[i].isMapped()) {
                // skip readonly relations
                if (!cols[i].isIdField() && !cols[i].isPrototypeField()) {
                    Relation rel = cols[i].getRelation();

                    if (rel.readonly) {
                        continue;
                    }
                }

                if (needsComma) {
                    b1.append(", "); //$NON-NLS-1$
                    b2.append(", "); //$NON-NLS-1$
                }
                b1.append(this.getDbSource().getConnection().getMetaData().getIdentifierQuoteString() +
                        cols[i].getName() +
                        this.getDbSource().getConnection().getMetaData().getIdentifierQuoteString());
                b2.append("?"); //$NON-NLS-1$
                needsComma = true;
            }
        }

        b1.append(b2.toString());
        b1.append(" )"); //$NON-NLS-1$

        // cache rendered string for later calls.
        ins = this.insertString = b1.toString();

        return ins;
    }


    /**
     *
     *
     * @return ...
     */
    public StringBuffer getUpdate() {
        String upd = this.updateString;

        if (upd != null) {
            return new StringBuffer(upd);
        }

        StringBuffer s = new StringBuffer("UPDATE "); //$NON-NLS-1$

        // append the name of the table to use for insert/update operations
        s.append(getInsertUpdateTableName());
        s.append(" SET "); //$NON-NLS-1$

        // cache rendered string for later calls.
        this.updateString = s.toString();

        return s;
    }

    /**
     *  Return true if values for the column identified by the parameter need
     *  to be quoted in SQL queries.
     *
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    public boolean needsQuotes(String columnName) throws SQLException, NoDriverException {
        if ((this.tableName == null) && (this.parentMapping != null)) {
            return this.parentMapping.needsQuotes(columnName);
        }
        DbColumn col = getColumn(columnName);
        // This is not a mapped column. In case of doubt, add quotes.
        if (col == null) {
            return true;
        }
        return col.needsQuotes();
    }

    /**
     * Add constraints to select query string to join object references
     */
    public void addJoinConstraints(StringBuffer s, String pre) {
        boolean isOracle = isOracle();
        String prefix = pre;

        if (!isOracle) {
            // constraints have already been rendered by getSelect()
            return;
        }

        for (int i = 0; i < this.joins.length; i++) {
            if (!this.joins[i].otherType.isRelational()) {
                continue;
            }
            s.append(prefix);
            this.joins[i].renderJoinConstraints(s, isOracle);
            prefix = " AND "; //$NON-NLS-1$
        }
    }

    /**
     * Is the database behind this an Oracle db?
     *
     * @return true if the dbsource is using an oracle JDBC driver
     */
    public boolean isOracle() {
        if (this.dbSource != null) {
            return this.dbSource.isOracle();
        }
        if (this.parentMapping != null) {
            return this.parentMapping.isOracle();
        }
        return false;
    }

    /**
     * Is the database behind this a MySQL db?
     *
     * @return true if the dbsource is using a MySQL JDBC driver
     */
    public boolean isMySQL() {
        if (this.dbSource != null) {
            return this.dbSource.isMySQL();
        }
        if (this.parentMapping != null) {
            return this.parentMapping.isMySQL();
        }
        return false;
    }

    /**
     * Is the database behind this a PostgreSQL db?
     *
     * @return true if the dbsource is using a PostgreSQL JDBC driver
     */
    public boolean isPostgreSQL() {
        if (this.dbSource != null) {
            return this.dbSource.isPostgreSQL();
        }
        if (this.parentMapping != null) {
            return this.parentMapping.isPostgreSQL();
        }
        return false;
    }

    /**
     * Is the database behind this a H2 db?
     *
     * @return true if the dbsource is using a H2 JDBC driver
     */
    public boolean isH2() {
        if (this.dbSource != null) {
            return this.dbSource.isH2();
        }
        if (this.parentMapping != null) {
            return this.parentMapping.isH2();
        }
        return false;
    }

    /**
     * Is the database behind this a SQLite db?
     *
     * @return true if the dbsource is using a SQLite JDBC driver
     */
    public boolean isSQLite() {
        if (dbSource != null) {
            return dbSource.isSQLite();
        }
        if (parentMapping != null) {
            return parentMapping.isSQLite();
        }
        return false;
    }

    /**
     * Return a string representation for this DbMapping
     *
     * @return a string representation
     */
    @Override
    public String toString() {
        if (this.typename == null) {
            return "[unspecified internal DbMapping]"; //$NON-NLS-1$
        }
        return ("[" + this.app.getName() + "." + this.typename + "]");   //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$
    }

    /**
     * Get the last time something changed in the Mapping
     *
     * @return time of last mapping change
     */
    public long getLastTypeChange() {
        return this.lastTypeChange;
    }

    /**
     * Get the last time something changed in our data
     *
     * @return time of last data change
     */
    public long getLastDataChange() {
        // refer to parent mapping if it uses the same db/table
        if (inheritsStorage()) {
            return this.parentMapping.getLastDataChange();
        }
        return this.lastDataChange;
    }

    /**
     * Set the last time something changed in the data, propagating the event
     * to mappings that depend on us through an additionalTables switch.
     */
    public void setLastDataChange() {
        // forward data change timestamp to storage-compatible parent mapping
        if (inheritsStorage()) {
            this.parentMapping.setLastDataChange();
        } else {
            this.lastDataChange += 1;
            // propagate data change timestamp to mappings that depend on us
            if (!this.dependentMappings.isEmpty()) {
                Iterator it = this.dependentMappings.iterator();
                while(it.hasNext()) {
                    DbMapping dbmap = (DbMapping) it.next();
                    dbmap.setIndirectDataChange();
                }
            }
        }
    }

    /**
     * Set the last time something changed in the data. This is already an indirect
     * data change triggered by a mapping we depend on, so we don't propagate it to
     * mappings that depend on us through an additionalTables switch.
     */
    protected void setIndirectDataChange() {
        // forward data change timestamp to storage-compatible parent mapping
        if (inheritsStorage()) {
            this.parentMapping.setIndirectDataChange();
        } else {
            this.lastDataChange += 1;
        }
    }

    /**
     * Helper method to generate a new ID. This is only used in the special case
     * when using the select(max) method and the underlying table is still empty.
     *
     * @param dbmax the maximum value already stored in db
     * @return a new and hopefully unique id
     */
    protected synchronized long getNewID(long dbmax) {
        // refer to parent mapping if it uses the same db/table
        if (inheritsStorage()) {
            return this.parentMapping.getNewID(dbmax);
        }
        this.lastID = Math.max(dbmax + 1, this.lastID + 1);
        return this.lastID;
    }

    /**
     * Return an enumeration of all properties defined by this db mapping.
     *
     * @return the property enumeration
     */
    public Enumeration getPropertyEnumeration() {
        HashSet set = new HashSet();

        collectPropertyNames(set);

        final Iterator it = set.iterator();

        return new Enumeration() {
                public boolean hasMoreElements() {
                    return it.hasNext();
                }

                public Object nextElement() {
                    return it.next();
                }
            };
    }

    /**
     * Collect a set of all properties defined by this db mapping
     *
     * @param basket the set to put properties into
     */
    private void collectPropertyNames(HashSet basket) {
        // fetch propnames from parent mapping first, than add our own.
        if (this.parentMapping != null) {
            this.parentMapping.collectPropertyNames(basket);
        }

        if (!this.prop2db.isEmpty()) {
            basket.addAll(this.prop2db.keySet());
        }
    }

    /**
     * Return the name of the prototype which specifies the storage location
     * (dbsource + tablename) for this type, or null if it is stored in the embedded
     * db.
     */
    public String getStorageTypeName() {
        if (inheritsStorage()) {
            return this.parentMapping.getStorageTypeName();
        }
        return (getDbSourceName() == null) ? null : this.typename;
    }

    /**
     * Check whether this DbMapping inherits its storage location from its
     * parent mapping. The raison d'etre for this is that we need to detect
     * inherited storage even if the dbsource and table are explicitly set
     * in the extended mapping.
     *
     * @return true if this mapping shares its parent mapping storage
     */
    protected boolean inheritsStorage() {
        // note: tableName and dbSourceName are nulled out in update() if they
        // are inherited from the parent mapping. This way we know that
        // storage is not inherited if either of them is not null.
        return isRelational() && this.parentMapping != null
                && this.tableName == null && this.dbSourceName == null;
    }

    /**
     * Static utility method to check whether two DbMappings use the same storage.
     *
     * @return true if both use the embedded database or the same relational table.
     */
    public static boolean areStorageCompatible(DbMapping dbm1, DbMapping dbm2) {
        if (dbm1 == null)
            return dbm2 == null || !dbm2.isRelational();
        return dbm1.isStorageCompatible(dbm2);
    }

    /**
     * Tell if this DbMapping uses the same storage as the given DbMapping.
     *
     * @return true if both use the embedded database or the same relational table.
     */
    public boolean isStorageCompatible(DbMapping other) {
        if (other == null) {
            return !isRelational();
        } else if (other == this) {
            return true;
        } else if (isRelational()) {
            return getTableName().equals(other.getTableName()) &&
                   getDbSource().equals(other.getDbSource());
        }

        return !other.isRelational();
    }

    /**
     *  Return true if this db mapping represents the prototype indicated
     *  by the string argument, either itself or via one of its parent prototypes.
     */
    public boolean isInstanceOf(String other) {
        if ((this.typename != null) && this.typename.equals(other)) {
            return true;
        }

        DbMapping p = this.parentMapping;

        while (p != null) {
            if ((p.typename != null) && p.typename.equals(other)) {
                return true;
            }

            p = p.parentMapping;
        }

        return false;
    }

    /**
     * Get the mapping we inherit from, or null
     *
     * @return the parent DbMapping, or null
     */
    public DbMapping getParentMapping() {
        return this.parentMapping;
    }

    /**
     * Get our ResourceProperties
     *
     * @return our properties
     */
    public Properties getProperties() {
        return this.props;
    }

    public Properties getSubProperties(String prefix) {
        if (this.props.get(prefix) instanceof Properties) {
            return (Properties) this.props.get(prefix);
        } else if (this.props instanceof ResourceProperties) {
            return ((ResourceProperties) this.props).getSubProperties(prefix + "."); //$NON-NLS-1$
        } else {
            Properties subprops = new Properties();
            prefix = prefix + "."; //$NON-NLS-1$
            Iterator it = this.props.entrySet().iterator();
            int prefixLength = prefix.length();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                String key = entry.getKey().toString();
                if (key.regionMatches(false, 0, prefix, 0, prefixLength)) {
                    subprops.put(key.substring(prefixLength), entry.getValue());
                }
            }
            return subprops;
        }
    }

    /**
     * Register a DbMapping that depends on this DbMapping, so that collections of other mapping
     * should be reloaded if data on this mapping is updated.
     *
     * @param dbmap the DbMapping that depends on us
     */
    protected void addDependency(DbMapping dbmap) {
        this.dependentMappings.add(dbmap);
    }

    /**
     * Append a sql-condition for the given column which must have
     * one of the values contained inside the given Set to the given
     * StringBuffer.
     * @param q the StringBuffer to append to
     * @param column the column which must match one of the values
     * @param values the list of values
     *
     * @throws SQLException
     * @throws NoDriverException  if the JDBC driver could not be loaded or is unusable
     */
    protected void appendCondition(StringBuffer q, String column, String[] values)
            throws SQLException, NoDriverException {
        if (values.length == 1) {
            appendCondition(q, column, values[0]);
            return;
        }
        if (column.indexOf('(') == -1 && column.indexOf('.') == -1) {
            q.append(getTableName()).append("."); //$NON-NLS-1$
        }
        q.append(column).append(" in ("); //$NON-NLS-1$

        if (needsQuotes(column)) {
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    q.append(", "); //$NON-NLS-1$
                q.append("'").append(escapeString(values[i])).append("'");  //$NON-NLS-1$//$NON-NLS-2$
            }
        } else {
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    q.append(", "); //$NON-NLS-1$
                q.append(checkNumber(values[i]));
            }
        }
        q.append(")"); //$NON-NLS-1$
    }

    /**
     * Append a sql-condition for the given column which must have
     * the value given to the given StringBuffer.
     * @param q the StringBuffer to append to
     * @param column the column which must match one of the values
     * @param val the value
     *
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    protected void appendCondition(StringBuffer q, String column, String val) 
            throws SQLException, NoDriverException {
        // delegate, assuming non insert/update operation
        appendCondition(q, column, val, false);
    }
    
    /**
     * Append a sql-condition for the given column which must have
     * the value given to the given StringBuffer.
     * @param q the StringBuffer to append to
     * @param column the column which must match one of the values
     * @param val the value
     * @param insertUpdate
     *  If to append the operation for an insert/update operation, in which case the identifier column will
     *  be qualified with the name of the table to use for insert/update operations instead of the name of
     *  the table to use for select operations.
     *
     * @throws SQLException
     * @throws NoDriverException if the JDBC driver could not be loaded or is unusable
     */
    protected void appendCondition(StringBuffer q, String column, String val, boolean insertUpdate)
            throws SQLException, NoDriverException {
        if (column.indexOf('(') == -1 && column.indexOf('.') == -1) {
            q.append(insertUpdate ? getInsertUpdateTableName() : getTableName()).append("."); //$NON-NLS-1$
        }
        q.append(column).append(" = "); //$NON-NLS-1$

        if (needsQuotes(column)) {
            q.append("'").append(escapeString(val)).append("'"); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            q.append(checkNumber(val));
        }
    }

    /**
     * a utility method to escape single quotes used for inserting
     * string-values into relational databases.
     * Searches for "'" characters and escapes them by duplicating them (= "''")
     * @param value the string to escape
     * @return the escaped string
     */
    static String escapeString(Object value) {
        String str = value == null ? null : value.toString();
        if (str == null) {
            return null;
        } else if (str.indexOf('\'') < 0 && str.indexOf('\\') < 0) {
            return str;
        }

        int l = str.length();
        StringBuffer sbuf = new StringBuffer(l + 10);

        for (int i = 0; i < l; i++) {
            char c = str.charAt(i);

            if (c == '\'') {
                sbuf.append("\\'"); //$NON-NLS-1$
            } else if (c == '\\') {
                sbuf.append("\\\\"); //$NON-NLS-1$
            } else {
                sbuf.append(c);
            }
        }
        return sbuf.toString();
    }

    /**
     * Utility method to check whether the argument is a number literal.
     * @param value a string representing a number literal
     * @return the argument, if it conforms to the number literal syntax
     * @throws IllegalArgumentException if the argument does not represent a number
     */
    static String checkNumber(Object value) throws IllegalArgumentException {
        String str = value == null ? null : value.toString();
        if (str == null) {
            return null;
        }
        str = str.trim();
        if (str.matches("(?:\\+|\\-)??\\d+(?:\\.\\d+)??")) { //$NON-NLS-1$
            return str;
        }
        throw new IllegalArgumentException(Messages.getString("DbMapping.22") + str); //$NON-NLS-1$
    }

    /**
     * Find if this DbMapping describes a virtual node (collection, mountpoint, groupnode)
     * @return true if this instance describes a virtual node.
     */
    public boolean isVirtual() {
        return this.isVirtual;
    }

    /**
     * Find if this DbMapping describes a group node.
     * @return true if this instance describes a group node.
     */
    public boolean isGroup() {
        return this.isGroup;
    }

    /**
     * Find whether a node with this DbMapping must be stored in the database.
     * This is true if this mapping defines a non-virtual node, or a virtual
     * node with non-relational child objects.
     * @return true if this node needs to be stored in the db, false otherwise
     */
    public boolean needsPersistence() {
        DbMapping submap = getSubnodeMapping();
        return !this.isVirtual || (submap != null && !submap.isRelational());
    }
}
