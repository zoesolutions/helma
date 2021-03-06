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

package helma.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import helma.framework.core.Application;
import helma.framework.repository.RepositoryInterface;
import helma.framework.repository.ResourceInterface;

/**
 *  A property dictionary that is updated from property resources
 */
public class ResourceProperties extends Properties {

    private static final long serialVersionUID = -2258056784572269727L;

    // Delay between checks
    private final long CACHE_TIME = 1500L;

    // Default properties. Note that in contrast to java.util.Properties,
    // defaultProperties are copied statically to ourselves in update(), so
    // there's no need to check them in retrieval methods.
    protected ResourceProperties defaultProperties;

    // Defines wether keys are case-sensitive or not
    private boolean ignoreCase = true;

    // Cached checksum of last check
    private long lastChecksum = 0;

    // Time of last check
    private long lastCheck = 0;

    // Time porperties were last modified
    private long lastModified = System.currentTimeMillis();

    // Application where to fetch additional resources
    private Application app;

    // Name of possible resources to fetch from the applications's repositories
    private String resourceName;

    // Sorted map of resources
    private Set resources;

    // lower case key to original key mapping for case insensitive lookups
    private Properties keyMap = new Properties();

    // prefix for sub-properties
    private String prefix;

    // parent properties for sub-properties
    private ResourceProperties parentProperties;

    /**
     * Constructs an empty ResourceProperties
     * Resources must be added manually afterwards
     */
    public ResourceProperties() {
        // TODO: we can't use TreeSet because we don't have the app's resource comparator
        // Since resources don't implement Comparable, we can't add them to a "naked" TreeSet
        // As a result, resource ordering is random when updating.
        this.resources = new LinkedHashSet();
    }

    /**
     * Constructs an empty ResourceProperties
     * Resources must be added manually afterwards
     */
    public ResourceProperties(Application app) {
        this.resources = new TreeSet(app.getResourceComparator());
    }

    /**
     * Constructs a ResourceProperties retrieving resources from the given
     * application using the given name to fetch resources
     * @param app application to fetch resources from
     * @param resourceName name to use when fetching resources from the application
     */
    public ResourceProperties(Application app, String resourceName) {
        this.app = app;
        this.resourceName = resourceName;
        this.resources = new TreeSet(app.getResourceComparator());
    }

    /**
     * Constructs a ResourceProperties retrieving resources from the given
     * application using the given name to fetch resources and falling back
     * to the given default properties
     * @param app application to fetch resources from
     * @param resourceName name to use when fetching resources from the application
     * @param defaultProperties default properties
     */
    public ResourceProperties(Application app, String resourceName,
                              ResourceProperties defaultProperties) {
        this(app, resourceName);
        this.defaultProperties = defaultProperties;
        forceUpdate();
    }

    /**
     * Constructs a ResourceProperties retrieving resources from the given
     * application using the given name to fetch resources and falling back
     * to the given default properties
     * @param app application to fetch resources from
     * @param resourceName name to use when fetching resources from the application
     * @param defaultProperties default properties
     * @param ignoreCase ignore case for property keys, setting all keys to lower case
     */
    public ResourceProperties(Application app, String resourceName,
                              ResourceProperties defaultProperties,
                              boolean ignoreCase) {
        this(app, resourceName);
        this.defaultProperties = defaultProperties;
        this.ignoreCase = ignoreCase;
        forceUpdate();
    }

    /**
     * Constructs a properties object containing all entries where the key matches
     * the given string prefix from the source map to the target map, cutting off
     * the prefix from the original key.
     * @see #getSubProperties(String)
     * @param parentProperties the parent properties
     * @param prefix the property name prefix
     */
    private ResourceProperties(ResourceProperties parentProperties, String prefix) {
        this.parentProperties = parentProperties;
        this.prefix = prefix;
        this.resources = new LinkedHashSet();
        setIgnoreCase(parentProperties.ignoreCase);
        forceUpdate();
    }

    /**
     * Updates the properties regardless of an actual need
     */
    private void forceUpdate() {
        this.lastChecksum = -1;
        update();
    }

    /**
     * Sets the default properties and updates all properties
     * @param defaultProperties default properties
     */
    public void setDefaultProperties(ResourceProperties defaultProperties) {
        this.defaultProperties = defaultProperties;
        update();
    }

    /**
     * Adds a resource to the list of resources and updates all properties if
     * needed
     * @param resource resource to add
     */
    public void addResource(ResourceInterface resource) {
        if (resource != null && !this.resources.contains(resource)) {
            this.resources.add(resource);
            forceUpdate();
        }
    }

    /**
     * Removes a resource from the list of resources and updates all properties
     * if needed
     * @param resource resource to remove
     */
    public void removeResource(ResourceInterface resource) {
        if (this.resources.contains(resource)) {
            this.resources.remove(resource);
            forceUpdate();
        }
    }

    /**
     * Get an iterator over the properties' resources
     * @return iterator over the properties' resources
     */
    public Iterator getResources() {
        return this.resources.iterator();
    }

    /**
     * Updates all properties if there is a need to update
     */
    public synchronized void update() {
        // set lastCheck first to reduce risk of recursive calls
        this.lastCheck = System.currentTimeMillis();
        if (getChecksum() != this.lastChecksum) {
            // First collect properties into a temporary collection,
            // in a second step copy over new properties,
            // and in the final step delete properties which have gone.
            ResourceProperties temp = new ResourceProperties();
            temp.setIgnoreCase(this.ignoreCase);

            // first of all, properties are load from default properties
            if (this.defaultProperties != null) {
                this.defaultProperties.update();
                temp.putAll(this.defaultProperties);
            }

            // next we try to load properties from the application's
            // repositories, if we belong to any application
            if (this.resourceName != null) {
                Iterator iterator = this.app.getRepositories().iterator();
                while (iterator.hasNext()) {
                    try {
                        RepositoryInterface repository = (RepositoryInterface) iterator.next();
                        ResourceInterface res = repository.getResource(this.resourceName);
                        if (res != null && res.exists()) {
							InputStreamReader reader = new InputStreamReader(
									res.getInputStream());
							temp.load(reader);
							reader.close();
                        }
                    } catch (IOException iox) {
                        iox.printStackTrace();
                    }
                }
            }

            // if these are subproperties, reload them from the parent properties
            if (this.parentProperties != null && this.prefix != null) {
                this.parentProperties.update();
                Iterator it = this.parentProperties.entrySet().iterator();
                int prefixLength = this.prefix.length();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    String key = entry.getKey().toString();
                    if (key.regionMatches(this.ignoreCase, 0, this.prefix, 0, prefixLength)) {
                        temp.put(key.substring(prefixLength), entry.getValue());
                    }
                }

            }

            // at last we try to load properties from the resource list
            if (this.resources != null) {
                Iterator iterator = this.resources.iterator();
                while (iterator.hasNext()) {
                    try {
                        ResourceInterface res = (ResourceInterface) iterator.next();
                        if (res.exists()) {
							InputStreamReader reader = new InputStreamReader(
									res.getInputStream());
							temp.load(reader);
							reader.close();
                        }
                    } catch (IOException iox) {
                        iox.printStackTrace();
                    }
                }
            }

            // Copy over new properties ...
            putAll(temp);
            // ... and remove properties which have been removed.
            Iterator it = super.keySet().iterator();
            while (it.hasNext()) {
                if (!temp.containsKey(it.next())) {
                    it.remove();
                }
            }
            // copy new up-to-date keyMap to ourself
            this.keyMap = temp.keyMap;

            this.lastChecksum = getChecksum();
            this.lastCheck = this.lastModified = System.currentTimeMillis();
        }
    }

    /**
     * Extract all entries where the key matches the given string prefix from
     * the source map to the target map, cutting off the prefix from the original key.
     * The ignoreCase property is inherited and also considered when matching keys
     * against the prefix.
     *
     * @param prefix the string prefix to match against
     * @return a new subproperties instance
     */
    public ResourceProperties getSubProperties(String prefix) {
        if (prefix == null) {
            throw new NullPointerException("prefix"); //$NON-NLS-1$
        }
        return new ResourceProperties(this, prefix);
    }

    /**
     * Checks wether the given object is in the value list
     * @param value value to look for
     * @return true if the value is found in the value list
     */
    @Override
    public synchronized boolean contains(Object value) {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return super.contains(value.toString());
    }

    /**
     * Checks wether the given object is in the key list
     * @param key key to look for
     * @return true if the key is found in the key list
     */
    @Override
    public synchronized boolean containsKey(Object key) {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        if (this.ignoreCase) {
            return this.keyMap.containsKey(key.toString().toLowerCase());
        }
        return super.containsKey(key.toString());
    }

    /**
     * Returns an enumeration of all values
     * @return values enumeration
     */
    @Override
    public synchronized Enumeration elements() {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return super.elements();
    }

    /**
     * Returns a value in this list fetched by the given key
     * @param key key to use for fetching the value
     * @return value belonging to the given key
     */
    @Override
    public synchronized Object get(Object key) {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        String strkey = key.toString();
        if (this.ignoreCase) {
            strkey = this.keyMap.getProperty(strkey.toLowerCase());
            if (strkey == null)
                return null;
        }
        return super.get(strkey);
    }

    /**
     * Returns the date the resources were last modified
     * @return last modified date
     */
    public long lastModified() {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return this.lastModified;
    }

    /**
     * Returns a checksum for all resources
     * @return checksum
     */
    public long getChecksum() {
        long checksum = 0;

        if (this.resourceName != null) {
            Iterator iterator = this.app.getRepositories().iterator();
            while (iterator.hasNext()) {
                RepositoryInterface repository = (RepositoryInterface) iterator.next();
                ResourceInterface resource = repository.getResource(this.resourceName);
                if (resource != null) {
                    checksum += resource.lastModified();
                }
            }
        }

        if (this.resources != null) {
            Iterator iterator = this.resources.iterator();
            while (iterator.hasNext()) {
                checksum += ((ResourceInterface) iterator.next()).lastModified();
            }
        }

        if (this.defaultProperties != null) {
            checksum += this.defaultProperties.getChecksum();
        }

        return checksum;
    }

    /**
     * Returns a value in the list fetched by the given key or a default value
     * if no corresponding key is found
     * @param key key to use for fetching the value
     * @param defaultValue default value to return if key is not found
     * @return spiecific value or default value if not found
     */
    @Override
    public String getProperty(String key, String defaultValue) {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        if (this.ignoreCase) {
            key = this.keyMap.getProperty(key.toLowerCase());
            if (key == null)
                return defaultValue;
        }
        return super.getProperty(key, defaultValue);
    }

    /**
     * Returns a value in this list fetched by the given key
     * @param key key to use for fetching the value
     * @return value belonging to the given key
     */
    @Override
    public String getProperty(String key) {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        if (this.ignoreCase) {
            key = this.keyMap.getProperty(key.toLowerCase());
            if (key == null)
                return null;
        }
        return super.getProperty(key);
    }

    /**
     * Checks wether the properties list is empty
     * @return true if the properties list is empty
     */
    @Override
    public synchronized boolean isEmpty() {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return super.isEmpty();
    }

    /**
     * Checks wether case-sensitivity is ignored for keys
     * @return true if case-sensitivity is ignored for keys
     */
    public boolean isIgnoreCase() {
        return this.ignoreCase;
    }

    /**
     * Returns an enumeration of all keys
     * @return keys enumeration
     */
    @Override
    public synchronized Enumeration keys() {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return super.keys();
    }

    /**
     * Returns a set of all keys
     * @return keys set
     */
    @Override
    public Set keySet() {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return super.keySet();
    }

    /**
     * Puts a new key-value pair into the properties list
     * @param key key
     * @param value value
     * @return the old value, if an old value got replaced
     */
    @Override
    public synchronized Object put(Object key, Object value) {
        if (value instanceof String) {
            value = ((String) value).trim();
        }
        String strkey = key.toString();
        if (this.ignoreCase) {
            this.keyMap.put(strkey.toLowerCase(), strkey);
        }
        return super.put(strkey, value);
    }

    /**
     * Removes a key-value pair from the properties list
     * @param key key
     * @return the old value
     */
    @Override
    public synchronized Object remove(Object key) {
        String strkey = key.toString();
        if (this.ignoreCase) {
            strkey = (String) this.keyMap.remove(strkey.toLowerCase());
            if (strkey == null)
                return null;
        }
        return super.remove(strkey);
    }

    /**
     * Changes how keys are handled
     * @param ignore true if to ignore case-sensitivity for keys
     */
    public void setIgnoreCase(boolean ignore) {
        if (!super.isEmpty()) {
            throw new RuntimeException(Messages.getString("ResourceProperties.0")); //$NON-NLS-1$
        }
        this.ignoreCase = ignore;
    }

    /**
     * Returns the number of peroperties in the list
     * @return number of properties
     */
    @Override
    public synchronized int size() {
        if ((System.currentTimeMillis() - this.lastCheck) > this.CACHE_TIME) {
            update();
        }
        return super.size();
    }

    /**
     * Overwrite clear() to also empty the key map.
     */
    @Override
    public synchronized void clear() {
        this.keyMap.clear();
        super.clear();
    }

    /**
     * Compares this ResourceProperties instance to the one passed
     * as argument. Note that in contrast to Hashtable.equals this method
     * isn't synchronized to avoid deadlocks (which can happen in eg.
     * DbSource.equals), and the comparison might return a wrong result
     * if one of the two instances is modified during this method call. This
     * method however doesn't throw a ConcurrentModificationException.
     *
     * @param  o object to be compared for equality with this instance
     * @return true if the specified Object is equal to this instance
     * @see Hashtable#equals(Object)
     */
    @Override
    public synchronized String toString() {
        return super.toString();
    }


}
