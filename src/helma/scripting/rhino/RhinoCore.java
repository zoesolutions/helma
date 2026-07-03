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

package helma.scripting.rhino;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.eclipse.wst.jsdt.debug.rhino.debugger.RhinoDebugger;
import org.mozilla.javascript.BaseFunction;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextAction;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.JavaScriptException;
import org.mozilla.javascript.LazilyLoadedCtor;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.ScriptRuntime;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;
import org.mozilla.javascript.WrapFactory;
import org.mozilla.javascript.Wrapper;
import org.mozilla.javascript.commonjs.module.RequireBuilder;
import org.mozilla.javascript.commonjs.module.provider.StrongCachingModuleScriptProvider;
import org.mozilla.javascript.tools.debugger.ScopeProvider;

import helma.framework.core.Application;
import helma.framework.core.Prototype;
import helma.framework.core.Skin;
import helma.framework.repository.ResourceInterface;
import helma.objectmodel.NodeInterface;
import helma.objectmodel.PropertyInterface;
import helma.objectmodel.db.DbMapping;
import helma.objectmodel.db.NodeHandle;
import helma.scripting.ScriptingEngineInterface;
import helma.scripting.ScriptingException;
import helma.util.CacheMap;
import helma.util.ResourceProperties;
import helma.util.SystemMap;
import helma.util.WeakCacheMap;
import helma.util.WrappedMap;

/**
 * This is the implementation of ScriptingEnvironment for the Mozilla Rhino EcmaScript interpreter.
 */
public final class RhinoCore implements ScopeProvider {
    // the application we're running in
    public final Application app;

    // our context factory
    ContextFactory contextFactory;

    // the global object
    GlobalObject global;

    // caching table for JavaScript object wrappers
    CacheMap wrappercache;

    // table containing JavaScript prototypes
    Hashtable prototypes;

    // timestamp of last type update
    volatile long lastUpdate = 0;

    // the wrap factory
    WrapFactory wrapper;

    // the prototype for HopObject
    ScriptableObject hopObjectProto;

    // the prototype for path objects
    PathWrapper pathProto;

    // Any error that may have been found in global code
    String globalError;

    // the debugger, if active
    RhinoDebugger debugger = null;

    // optimization level for rhino engine, ranges from -1 to 9
    int optLevel = 0;

    // language version - default to JS 1.8
    int languageVersion = 180;

    // debugger/tracer flags
    boolean hasDebugger = false;
    String debuggerSettings;
    boolean hasTracer = false;
    boolean hasProfiler = false;
    private boolean isInitialized = false;

    // dynamic portion of the type check sleep that grows
    // as the app remains unchanged
    long updateSnooze = 500;

    /**
     *  Create a Rhino evaluator for the given application and request evaluator.
     */
    public RhinoCore(Application app) {
        this.app = app;
        this.wrappercache = new WeakCacheMap(500);
        this.prototypes = new Hashtable();
        this.contextFactory = new HelmaContextFactory();
        this.contextFactory.initApplicationClassLoader(app.getClassLoader());
    }

    /**
     *  Initialize the evaluator, making sure the minimum type information
     *  necessary to bootstrap the rest is parsed.
     */
    protected synchronized void initialize() {

    	String debug = app.getProperty("rhino.debug"); //$NON-NLS-1$
        hasDebugger = debug != null && !"false".equalsIgnoreCase(debug); //$NON-NLS-1$
        if (hasDebugger)
          this.debuggerSettings = debug;
        hasTracer = "true".equalsIgnoreCase(app.getProperty("rhino.trace")); //$NON-NLS-1$//$NON-NLS-2$
        hasProfiler = "true".equalsIgnoreCase(app.getProperty("rhino.profile")); //$NON-NLS-1$//$NON-NLS-2$

        // Set default optimization level according to whether debugger is on
        if (this.hasDebugger || this.hasTracer || this.hasProfiler) {
            this.optLevel = -1;
        } else {
            String opt = this.app.getProperty("rhino.optlevel"); //$NON-NLS-1$
            if (opt != null) {
                try {
                    this.optLevel = Integer.parseInt(opt);
                } catch (Exception ignore) {
                    this.app.logError(Messages.getString("RhinoCore.0") + opt); //$NON-NLS-1$
                }
            }
        }
        String v = this.app.getProperty("rhino.languageVersion"); //$NON-NLS-1$
        if (v != null) {
            try {
                this.languageVersion = Integer.parseInt(v);
            } catch (Exception ignore) {
                this.app.logError(Messages.getString("RhinoCore.1") + v); //$NON-NLS-1$
            }
        }
        this.wrapper = new WrapMaker();
        this.wrapper.setJavaPrimitiveWrap(false);

        Context context = this.contextFactory.enterContext();

        try {
            // create global object
            this.global = new GlobalObject(this, this.app, false);
            // call the initStandardsObject in ImporterTopLevel so that
            // importClass() and importPackage() are set up.
            this.global.initStandardObjects(context, false);
            this.global.init();

            // Enable loading and exporting of CommonJS modules with require and module.exports, resp.
            // Inspiration: http://stackoverflow.com/a/30355409/5281580

            List<URI> commonJsPaths = new ArrayList<URI>();
            commonJsPaths.add(this.app.getAppDir().toURI());
            String commonJsAppPath = this.app.getProperty("commonjs.dir"); //$NON-NLS-1$

            if (commonJsAppPath != null) {
                File commonJsAppDir = new File(this.app.getAppDir(), commonJsAppPath);
                if (commonJsAppDir.isDirectory()) {
                    commonJsPaths.add(commonJsAppDir.toURI());
                }
            }

            // install the global require() function using our custom modules provider, so that 
            // CommonJS-style as well as NodeJS-style modules can be required
            new RequireBuilder()
                .setModuleScriptProvider(new CompiledOrInterpretedModuleScriptProvider(
                        new NodeModulesProvider(commonJsPaths, null)))
                .setSandboxed(true)
                .createRequire(context, global)
                .install(global);

            this.pathProto = new PathWrapper(this);

            this.hopObjectProto =  HopObject.init(this);
            // use lazy loaded constructors for all extension objects that
            // adhere to the ScriptableObject.defineClass() protocol
            new LazilyLoadedCtor(this.global, "Remote", //$NON-NLS-1$
                    "helma.scripting.rhino.extensions.XmlRpcObject", false); //$NON-NLS-1$
            JSAdapter.init(context, this.global, false);

            // add some convenience functions to string, date and number prototypes
            Scriptable stringProto = ScriptableObject.getClassPrototype(this.global, "String"); //$NON-NLS-1$
            stringProto.put("trim", stringProto, new StringTrim()); //$NON-NLS-1$

            Scriptable dateProto = ScriptableObject.getClassPrototype(this.global, "Date"); //$NON-NLS-1$
            dateProto.put("format", dateProto, new DateFormat()); //$NON-NLS-1$

            Scriptable numberProto = ScriptableObject.getClassPrototype(this.global, "Number"); //$NON-NLS-1$
            numberProto.put("format", numberProto, new NumberFormat()); //$NON-NLS-1$

            Collection protos = this.app.getPrototypes();
            for (Iterator i = protos.iterator(); i.hasNext();) {
                Prototype proto = (Prototype) i.next();
                initPrototype(proto);
            }

            // always fully initialize global prototype, because
            // we always need it and there's no chance to trigger
            // creation on demand.
            getPrototype("global"); //$NON-NLS-1$

        } catch (Exception e) {
            this.app.logError(Messages.getString("RhinoCore.2"), e); //$NON-NLS-1$
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            Context.exit();
            this.isInitialized = true;
        }
    }

    boolean isInitialized() {
        return this.isInitialized;
    }

    public void shutdown() {
        if (this.debugger != null) {
            try {
              contextFactory.removeListener(this.debugger);
              debugger.stop();
            } catch (Exception e) {
              e.printStackTrace();
            }
            this.debugger = null;
        }
    }

    void initDebugger(Context context) {
        context.setGeneratingDebug(true);
        try {
            if (this.debugger == null) {
                this.debugger = new RhinoDebugger(this.debuggerSettings);
                this.debugger.start();
                contextFactory.addListener(this.debugger);
            }
        } catch (Exception x) {
            this.app.logError(Messages.getString("RhinoCore.3"), x); //$NON-NLS-1$
        }
    }

    /**
     *   Initialize a prototype info without compiling its script files.
     *
     *  @param prototype the prototype to be created
     */
    protected synchronized TypeInfo initPrototype(Prototype prototype) {

        String name = prototype.getName();
        String lowerCaseName = prototype.getLowerCaseName();

        TypeInfo type = (TypeInfo) this.prototypes.get(lowerCaseName);

        // check if the prototype info exists already
        ScriptableObject op = (type == null) ? null : type.objProto;

        // if prototype info doesn't exist (i.e. is a standard prototype
        // built by HopExtension), create it.
        if (op == null) {
            if ("global".equals(lowerCaseName)) { //$NON-NLS-1$
                op = this.global;
            } else if ("hopobject".equals(lowerCaseName)) { //$NON-NLS-1$
                op = this.hopObjectProto;
            } else {
                op = new HopObject(name, this);
            }
            type = registerPrototype(prototype, op);
        }

        // Register a constructor for all types except global.
        // This will first create a new prototyped HopObject and then calls
        // the actual (scripted) constructor on it.
        if (!"global".equals(lowerCaseName)) { //$NON-NLS-1$
            try {
                new HopObjectCtor(name, this, op);
                op.setParentScope(this.global);
            } catch (Exception x) {
                this.app.logError(Messages.getString("RhinoCore.4") + name,  x); //$NON-NLS-1$
            }
        }

        return type;
    }

    /**
     *  Set up a prototype, parsing and compiling all its script files.
     *
     *  @param type the info, containing the object proto, last update time and
     *         the set of compiled functions properties
     */
    private synchronized void evaluatePrototype(final TypeInfo type) {

        type.prepareCompilation();
        final Prototype prototype = type.frameworkProto;

        // set the parent prototype in case it hasn't been done before
        // or it has changed...
        setParentPrototype(prototype, type);

        type.error = null;
        if ("global".equals(prototype.getLowerCaseName())) { //$NON-NLS-1$
            this.globalError = null;
        }

        this.contextFactory.call(new ContextAction() {
            public Object run(Context cx) {
                // loop through the prototype's code elements and evaluate them
                Iterator code = prototype.getCodeResources();
                while (code.hasNext()) {
                    evaluate(cx, type, (ResourceInterface) code.next());
                }
                return null;
            }
        });
        type.commitCompilation();
    }

    /**
     *  Set the parent prototype on the ObjectPrototype.
     *
     *  @param prototype the prototype spec
     *  @param type the prototype object info
     */
    protected void setParentPrototype(Prototype prototype, TypeInfo type) {
        String name = prototype.getName();
        String lowerCaseName = prototype.getLowerCaseName();

        if (!"global".equals(lowerCaseName) && !"hopobject".equals(lowerCaseName)) { //$NON-NLS-1$ //$NON-NLS-2$

            // get the prototype's prototype if possible and necessary
            TypeInfo parentType = null;
            Prototype parent = prototype.getParentPrototype();

            if (parent != null) {
                // see if parent prototype is already registered. if not, register it
                parentType = getPrototypeInfo(parent.getName());
            }

            if (parentType == null && !this.app.isJavaPrototype(name)) {
                // FIXME: does this ever occur?
                parentType = getPrototypeInfo("hopobject"); //$NON-NLS-1$
            }

            type.setParentType(parentType);
        }
    }

    /**
     *  This method is called before an execution context is entered to let the
     *  engine know it should update its prototype information. The update policy
     *  here is to check for update those prototypes which already have been compiled
     *  before. Others will be updated/compiled on demand.
     */
    public synchronized void updatePrototypes() throws IOException {
        if ((System.currentTimeMillis() - this.lastUpdate) < 1000L + this.updateSnooze + Long.parseLong(this.app.getProperty("updateDelay", "0"))) { //$NON-NLS-1$ //$NON-NLS-2$
            return;
        }

        // init prototypes and/or update prototype checksums
        this.app.typemgr.checkPrototypes();

        // get a collection of all prototypes (code directories)
        Collection protos = this.app.getPrototypes();

        // in order to respect inter-prototype dependencies, we try to update
        // the global prototype before all other prototypes, and parent
        // prototypes before their descendants.

        HashSet checked = new HashSet(protos.size() * 2);

        TypeInfo type = (TypeInfo) this.prototypes.get("global"); //$NON-NLS-1$

        if (type != null) {
            updatePrototype(type, checked);
        }

        for (Iterator i = protos.iterator(); i.hasNext();) {
            Prototype proto = (Prototype) i.next();

            if (checked.contains(proto)) {
                continue;
            }

            type = (TypeInfo) this.prototypes.get(proto.getLowerCaseName());

            if (type == null) {
                // a prototype we don't know anything about yet. Init local update info.
                initPrototype(proto);
            } else if (type.lastTypeInfoUpdate > -1) {
                // only need to update prototype if it has already been initialized.
                // otherwise, this will be done on demand.
                updatePrototype(type, checked);
            }
        }

        this.lastUpdate = System.currentTimeMillis();
        // max updateSnooze is 4 seconds, reached after 66.6 idle minutes
        long newSnooze = (this.lastUpdate - this.app.typemgr.getLastCodeUpdate()) / 1000;
        this.updateSnooze = Math.min(4000, Math.max(0, newSnooze));
    }

    /**
     * Check one prototype for updates. Used by <code>upatePrototypes()</code>.
     *
     * @param type the type info to check
     * @param checked a set of prototypes that have already been checked
     */
    private void updatePrototype(TypeInfo type, HashSet checked) {
        // first, remember prototype as updated
        checked.add(type.frameworkProto);

        if (type.parentType != null &&
                !checked.contains(type.parentType.frameworkProto)) {
            updatePrototype(type.getParentType(), checked);
        }

        // let the prototype check if its resources have changed
        type.frameworkProto.checkForUpdates();

        // and re-evaluate if necessary
        if (type.needsUpdate()) {
            evaluatePrototype(type);
        }
    }

    /**
     * A version of getPrototype() that retrieves a prototype and checks
     * if it is valid, i.e. there were no errors when compiling it. If
     * invalid, a ScriptingException is thrown.
     */
    public Scriptable getValidPrototype(String protoName) {
        if (this.globalError != null) {
            throw new EvaluatorException(this.globalError);
        }
        TypeInfo type = getPrototypeInfo(protoName);
        if (type != null) {
            if (type.hasError()) {
                throw new EvaluatorException(type.getError());
            }
            return type.objProto;
        }
        return null;
    }

    /**
     *  Get the object prototype for a prototype name and initialize/update it
     *  if necessary. The policy here is to update the prototype only if it
     *  hasn't been updated before, otherwise we assume it already was updated
     *  by updatePrototypes(), which is called for each request.
     */
    public Scriptable getPrototype(String protoName) {
        TypeInfo type = getPrototypeInfo(protoName);
        return type == null ? null : type.objProto;
    }

    /**
     * Get an array containing the property ids of all properties that were
     * compiled from scripts for the given prototype.
     *
     * @param protoName the name of the prototype
     * @return an array containing all compiled properties of the given prototype
     */
    public Map getPrototypeProperties(String protoName) {
        TypeInfo type = getPrototypeInfo(protoName);
        SystemMap map = new SystemMap();
        Iterator it = type.compiledProperties.iterator();
        while(it.hasNext()) {
            Object key = it.next();
            if (key instanceof String)
                map.put(key, type.objProto.get((String) key, type.objProto));
        }
        return map;
    }

    /**
     *  Private helper function that retrieves a prototype's TypeInfo
     *  and creates it if not yet created. This is used by getPrototype() and
     *  getValidPrototype().
     */
    private TypeInfo getPrototypeInfo(String protoName) {
        if (protoName == null) {
            return null;
        }

        TypeInfo type = (TypeInfo) this.prototypes.get(protoName.toLowerCase());

        // if type exists and hasn't been evaluated (used) yet, evaluate it now.
        // otherwise, it has already been evaluated for this request by updatePrototypes(),
        // which is called before a request is handled.
        if ((type != null) && (type.lastTypeInfoUpdate == -1)) {
            type.frameworkProto.checkForUpdates();

            if (type.needsUpdate()) {
                evaluatePrototype(type);
            }
        }

        return type;
    }

    /**
     * Register an object prototype for a prototype name.
     */
    private TypeInfo registerPrototype(Prototype proto, ScriptableObject op) {
        TypeInfo type = new TypeInfo(proto, op);
        this.prototypes.put(proto.getLowerCaseName(), type);
        return type;
    }

    /**
    * Check if an object has a function property (public method if it
    * is a java object) with that name.
    */
    public boolean hasFunction(String protoname, String fname) {
        // throws EvaluatorException if type has a syntax error
        Scriptable op = getValidPrototype(protoname);

        // if this is an untyped object return false
        if (op == null) {
            return false;
        }

        return ScriptableObject.getProperty(op, fname) instanceof Function;
    }

    /**
     *  Convert an input argument from Java to the scripting runtime
     *  representation.
     */
    public Object processXmlRpcArgument (Object arg) {
        if (arg == null)
            return null;
        if (arg instanceof Vector) {
            Vector v = (Vector) arg;
            Object[] a = v.toArray();
            for (int i=0; i<a.length; i++) {
                a[i] = processXmlRpcArgument(a[i]);
            }
            return Context.getCurrentContext().newArray(this.global, a);
        }
        if (arg instanceof Hashtable) {
            Hashtable t = (Hashtable) arg;
            for (Enumeration e=t.keys(); e.hasMoreElements(); ) {
                Object key = e.nextElement();
                t.put(key, processXmlRpcArgument(t.get(key)));
            }
            return Context.toObject(new SystemMap(t), this.global);
        }
        if (arg instanceof String)
            return arg;
        if (arg instanceof Number)
            return arg;
        if (arg instanceof Boolean)
            return arg;
        if (arg instanceof Date) {
            Date d = (Date) arg;
            Object[] args = { new Long(d.getTime()) };
            return Context.getCurrentContext().newObject(this.global, "Date", args); //$NON-NLS-1$
        }
        return Context.toObject(arg, this.global);
    }

    /**
     * convert a JavaScript Object object to a generic Java object stucture.
     */
    public Object processXmlRpcResponse (Object arg) {
        // unwrap if argument is a Wrapper
        if (arg instanceof Wrapper) {
            arg = ((Wrapper) arg).unwrap();
        }
        if (arg instanceof NativeObject) {
            NativeObject no = (NativeObject) arg;
            Object[] ids = no.getIds();
            Hashtable ht = new Hashtable(ids.length*2);
            for (int i=0; i<ids.length; i++) {
                if (ids[i] instanceof String) {
                    String key = (String) ids[i];
                    Object o = no.get(key, no);
                    if (o != null) {
                        ht.put(key, processXmlRpcResponse(o));
                    }
                }
            }
            return ht;
        } else if (arg instanceof NativeArray) {
            NativeArray na = (NativeArray) arg;
            Number n = (Number) na.get("length", na); //$NON-NLS-1$
            int l = n.intValue();
            Vector retval = new Vector(l);
            for (int i=0; i<l; i++) {
                retval.add(i, processXmlRpcResponse(na.get(i, na)));
            }
            return retval;
        } else if (arg instanceof Map) {
            Map map = (Map) arg;
            Hashtable ht = new Hashtable(map.size()*2);
            for (Iterator it=map.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                ht.put(entry.getKey().toString(),
                       processXmlRpcResponse(entry.getValue()));
            }
            return ht;
        } else if (arg instanceof Number) {
            Number n = (Number) arg;
            if (arg instanceof Float || arg instanceof Long) {
                return new Double(n.doubleValue());
            } else if (!(arg instanceof Double)) {
                return new Integer(n.intValue());
            }
        } else if (arg instanceof NodeInterface) {
            // interpret HopObject as object/dict
            NodeInterface n = (NodeInterface) arg;
            Hashtable ht = new Hashtable();
            Enumeration props = n.properties();
            while (props.hasMoreElements()) {
                String key = (String) props.nextElement();
                PropertyInterface prop = n.get(key);
                if (prop != null) {
                    ht.put(key, processXmlRpcResponse(prop.getValue()));
                }
            }
            return ht;
        } else if (arg instanceof Scriptable) {
            Scriptable s = (Scriptable) arg;
            if ("Date".equals(s.getClassName())) { //$NON-NLS-1$
                return new Date((long) ScriptRuntime.toNumber(s));
            }
        }
        return arg;
    }


    /**
     * Return the application we're running in
     */
    public Application getApplication() {
        return this.app;
    }


    /**
     *  Get a Script wrapper for any given object. If the object implements the PathElementInterface
     *  interface, the getPrototype method will be used to retrieve the name of the prototype
     * to use. Otherwise, a Java-Class-to-Script-Prototype mapping is consulted.
     */
    public Scriptable getElementWrapper(Object e) {
        WeakReference ref = (WeakReference) this.wrappercache.get(e);
        Wrapper wrapper = ref == null ? null : (Wrapper) ref.get();

        if (wrapper == null || wrapper.unwrap() != e) {
            // Gotta find out the prototype name to use for this object...
            String prototypeName = this.app.getPrototypeName(e);
            Scriptable op = getPrototype(prototypeName);

            if (op == null) {
                // no prototype found, return an unscripted wrapper
                wrapper = new NativeJavaObject(this.global, e, e.getClass());
            } else {
                wrapper = new JavaObject(this.global, e, prototypeName, op, this);
            }

            this.wrappercache.put(e, new WeakReference(wrapper));
        }

        return (Scriptable) wrapper;
    }

    /**
     *  Get a script wrapper for an instance of helma.objectmodel.NodeInterface
     */
    public Scriptable getNodeWrapper(NodeInterface node) {
        if (node == null) {
            return null;
        }

        HopObject hobj = (HopObject) this.wrappercache.get(node);

        if (hobj == null) {
            String protoname = node.getPrototype();
            Scriptable op = getValidPrototype(protoname);

            // no prototype found for this node
            if (op == null) {
                // maybe this object has a prototype name that has been
                // deleted, but the storage layer was able to set a
                // DbMapping matching the relational table the object
                // was fetched from.
                DbMapping dbmap = node.getDbMapping();
                if (dbmap != null && (protoname = dbmap.getTypeName()) != null) {
                    op = getValidPrototype(protoname);
                }

                // if not found, fall back to HopObject prototype
                if (op == null) {
                    protoname = "HopObject"; //$NON-NLS-1$
                    op = getValidPrototype("HopObject"); //$NON-NLS-1$
                }
            }

            hobj = new HopObject(protoname, this, node, op);
            this.wrappercache.put(node, hobj);
        }

        return hobj;
    }

    /**
     * Get a node wrapper for a node that may not have been fetched yet
     * @param handle a node handle
     * @return a wrapper for the node
     */
    public Scriptable getNodeWrapper(NodeHandle handle) {
        Scriptable hobj = (HopObject) this.wrappercache.get(handle);
        if (hobj != null) {
            return hobj;
        } else if (handle.hasNode()) {
            hobj = getNodeWrapper(handle.getNode(this.app.getWrappedNodeManager()));
        }

        if (hobj == null) {
            if (handle.getKey() == null) {
                return null;
            }
            String protoName = handle.getKey().getStorageName();
            Scriptable op = getValidPrototype(protoName);

            // no prototype found for this node
            if (op == null) {
                protoName = "HopObject"; //$NON-NLS-1$
                op = getValidPrototype("HopObject"); //$NON-NLS-1$s
            }
            hobj = new HopObject(protoName, this, handle, op);
        }
        this.wrappercache.put(handle, hobj);
        return hobj;
    }


    protected String postProcessHref(Object obj, String protoName, String href)
            throws UnsupportedEncodingException, IOException {
        // check if the app.properties specify a href-function to post-process the
        // basic href.
        String hrefFunction = this.app.getProperty("hrefFunction", null); //$NON-NLS-1$

        if (hrefFunction != null) {

            Object handler = obj;
            String proto = protoName;

            while (handler != null) {
                if (hasFunction(proto, hrefFunction)) {

                    // get the currently active rhino engine and invoke the function
                    RhinoEngine eng = RhinoEngine.getRhinoEngine();
                    Object result;

                    try {
                        result = eng.invoke(handler, hrefFunction,
                                               new Object[] {href},
                                               ScriptingEngineInterface.ARGS_WRAP_DEFAULT,
                                               false);
                    } catch (ScriptingException x) {
                        throw new EvaluatorException(Messages.getString("RhinoCore.5") + x); //$NON-NLS-1$
                    }

                    if (result == null) {
                        throw new EvaluatorException(Messages.getString("RhinoCore.6") + hrefFunction + //$NON-NLS-1$
                                                       Messages.getString("RhinoCore.7")); //$NON-NLS-1$
                    }

                    href = result.toString();
                    break;
                }
                handler = this.app.getParentElement(handler);
                proto = this.app.getPrototypeName(handler);

            }
        }

        // check if the app.properties specify a href-skin to post-process the
        // basic href.
        String hrefSkin = this.app.getProperty("hrefSkin", null); //$NON-NLS-1$

        if (hrefSkin != null) {
            // we need to post-process the href with a skin for this application
            // first, look in the object href was called on.
            Skin skin = null;
            Object handler = obj;
            // get the currently active rhino engine and render the skin
            RhinoEngine eng = RhinoEngine.getRhinoEngine();

            while (handler != null) {
                Prototype proto = this.app.getPrototype(handler);

                if (proto != null) {
                    skin = eng.getSkin(proto.getName(), hrefSkin);
                }

                if (skin != null) {
                    Scriptable param = Context.getCurrentContext().newObject(this.global);
                    param.put("path", param, href); //$NON-NLS-1$
                    href = skin.renderAsString(eng.getRequestEvaluator(), handler, param).trim();
                    break;
                }

                handler = this.app.getParentElement(handler);
            }
        }

        return href;
    }


    Properties scriptableToProperties(Scriptable obj) {
        Object[] ids = obj.getIds();
        Properties props = new ResourceProperties(this.app, null, null, true);
        for (int i = 0; i < ids.length; i++) {
            // we ignore non-string keys
            if (ids[i] instanceof String) {
                String key = (String) ids[i];
                Object value = obj.get(key, obj);
                // Normalize values to either null, string, or nested map
                if (value == Undefined.instance || value == Scriptable.NOT_FOUND) {
                    value = null;
                } else if (value instanceof Scriptable) {
                    value = scriptableToProperties((Scriptable) value);
                } else {
                    value = ScriptRuntime.toString(value);
                }
                props.put(key, value);
            }
        }
        return props;
    }

    /**
     * Get the RhinoCore instance associated with the current thread, or null
     * @return the RhinoCore instance associated with the current thread
     */
    public static RhinoCore getCore() {
        RhinoEngine eng = RhinoEngine.getRhinoEngine();
        return eng != null ? eng.core : null;
    }

    /////////////////////////////////////////////
    // skin related methods
    /////////////////////////////////////////////

    protected static Object[] unwrapSkinpath(Object[] skinpath) {
        if (skinpath != null) {
            for (int i=0; i<skinpath.length; i++) {
                if (skinpath[i] instanceof HopObject) {
                    skinpath[i] = ((HopObject) skinpath[i]).getNode();
                } else if (skinpath[i] instanceof Wrapper) {
                    skinpath[i] = ((Wrapper) skinpath[i]).unwrap();
                }
            }
        }
        return skinpath;
    }

    /**
     * Add a code resource to a given prototype by immediately compiling and evaluating it.
     *
     * @param typename the type this resource belongs to
     * @param code a code resource
     */
    public void injectCodeResource(String typename, final ResourceInterface code) {
        final TypeInfo type = (TypeInfo) this.prototypes.get(typename.toLowerCase());
        if (type == null || type.lastTypeInfoUpdate == -1)
            return;
        this.contextFactory.call(new ContextAction() {
            public Object run(Context cx) {
                evaluate(cx, type, code);
                return null;
            }
        });
    }

    ////////////////////////////////////////////////
    // private evaluation/compilation methods
    ////////////////////////////////////////////////
    private synchronized void evaluate(Context cx, TypeInfo type, ResourceInterface code) {
        String sourceName = code.getName();
        Reader reader = null;

        ResourceInterface previousCurrentResource = this.app.getCurrentCodeResource();
        this.app.setCurrentCodeResource(code);

        String encoding = this.app.getProperty("sourceCharset"); //$NON-NLS-1$

        try {
            Scriptable op = type.objProto;
            // do the update, evaluating the file
            if (sourceName.endsWith(".js")) { //$NON-NLS-1$
                reader = encoding == null ?
                        new InputStreamReader(code.getInputStream()) :
                        new InputStreamReader(code.getInputStream(), encoding);
                cx.evaluateReader(op, reader, sourceName, 1, null);
            } else if (sourceName.endsWith(".hac")) { //$NON-NLS-1$
                reader = new StringReader(HacHspConverter.convertHac(code, encoding));
                cx.evaluateReader(op, reader, sourceName, 0, null);
            } else if (sourceName.endsWith(".hsp")) { //$NON-NLS-1$
                reader = new StringReader(HacHspConverter.convertHsp(code, encoding));
                cx.evaluateReader(op, reader, sourceName, 0, null);
                reader = new StringReader(HacHspConverter.convertHspAsString(code, encoding));
                cx.evaluateReader(op, reader, sourceName, 0, null);
            }

        } catch (Exception e) {
            ScriptingException sx = new ScriptingException(e.getMessage(), e);
            this.app.logError(Messages.getString("RhinoCore.8") + sourceName, sx); //$NON-NLS-1$
            // mark prototype as broken
            if (type.error == null) {
                type.error = e.getMessage();
                if (type.error == null) {
                    type.error = e.toString();
                }
                if ("global".equals(type.frameworkProto.getLowerCaseName())) { //$NON-NLS-1$
                    this.globalError = type.error;
                }
                this.wrappercache.clear();
            }
        } finally {
            this.app.setCurrentCodeResource(previousCurrentResource);
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignore) {
                    // shouldn't happen
                }
            }
        }
    }

    /**
     *  Return the global scope of this RhinoCore.
     */
    public Scriptable getScope() {
        return this.global;
    }

    /**
     *  TypeInfo helper class
     */
    class TypeInfo {

        // the framework prototype object
        Prototype frameworkProto;

        // the JavaScript prototype for this type
        ScriptableObject objProto;

        // timestamp of last update. This is -1 so even an empty prototype directory
        // (with lastUpdate == 0) gets evaluated at least once, which is necessary
        // to get the prototype chain set.
        long lastTypeInfoUpdate = -1;

        // the parent prototype info
        TypeInfo parentType;

        // a set of property keys that were in script compilation.
        // Used to decide which properties should be removed if not renewed.
        Set compiledProperties;

        // a set of property keys that were present before first script compilation
        final Set predefinedProperties;

        String error;

        public TypeInfo(Prototype proto, ScriptableObject op) {
            this.frameworkProto = proto;
            this.objProto = op;
            // remember properties already defined on this object prototype
            this.compiledProperties = new HashSet();
            this.predefinedProperties = new HashSet();
            Object[] keys = op.getAllIds();
            for (int i = 0; i < keys.length; i++) {
                this.predefinedProperties.add(keys[i].toString());
            }
        }

        /**
         * If prototype implements PropertyRecorderInterface tell it to start
         * registering property puts.
         */
        public void prepareCompilation() {
            if (this.objProto instanceof PropertyRecorderInterface) {
                ((PropertyRecorderInterface) this.objProto).startRecording();
            }
            // mark this type as updated so injectCodeResource() knows it's initialized
            this.lastTypeInfoUpdate = this.frameworkProto.lastCodeUpdate();
        }

        /**
         * Compilation has been completed successfully - switch over to code
         * from temporary prototype, removing properties that haven't been
         * renewed.
         */
        public void commitCompilation() {
            // loop through properties defined on the prototype object
            // and remove thos properties which haven't been renewed during
            // this compilation/evaluation pass.
            if (this.objProto instanceof PropertyRecorderInterface) {

                PropertyRecorderInterface recorder = (PropertyRecorderInterface) this.objProto;

                recorder.stopRecording();
                Set changedProperties = recorder.getChangeSet();

                if (changedProperties != null) {
                    recorder.clearChangeSet();

                    // ignore all  properties that were defined before we started
                    // compilation. We won't manage these properties, even
                    // if they were set during compilation.
                    changedProperties.removeAll(this.predefinedProperties);

                    // remove all renewed properties from the previously compiled
                    // property names so we can remove those properties that were not
                    // renewed in this compilation
                    this.compiledProperties.removeAll(changedProperties);

                    boolean isGlobal = "global".equals(this.frameworkProto.getLowerCaseName()); //$NON-NLS-1$s

                    Iterator it = this.compiledProperties.iterator();
                    while (it.hasNext()) {
                        String key = (String) it.next();
                        if (isGlobal && (RhinoCore.this.prototypes.containsKey(key.toLowerCase())
                                || "JavaPackage".equals(key))) { //$NON-NLS-1$
                            // avoid removing HopObject constructor
                            this.predefinedProperties.add(key);
                            continue;
                        }
                        try {
                            this.objProto.setAttributes(key, 0);
                            this.objProto.delete(key);
                        } catch (Exception px) {
                            RhinoCore.this.app.logEvent(Messages.getString("RhinoCore.9")+key+Messages.getString("RhinoCore.10")+ //$NON-NLS-1$ //$NON-NLS-2$
                                    this.frameworkProto.getName());
                        }
                    }

                    // update compiled properties
                    this.compiledProperties = changedProperties;
                }
            }

            // mark this type as updated again so it reflects
            // resources added during compilation
            // lastUpdate = frameworkProto.lastCodeUpdate();

            // If this prototype defines a postCompile() function, call it
            Context cx = Context.getCurrentContext();
            try {
                Object fObj = ScriptableObject.getProperty(this.objProto,
                                                           "onCodeUpdate"); //$NON-NLS-1$
                if (fObj instanceof Function) {
                    Object[] args = {this.frameworkProto.getName()};
                    ((Function) fObj).call(cx, RhinoCore.this.global, this.objProto, args);
                }
            } catch (Exception x) {
                RhinoCore.this.app.logError(Messages.getString("RhinoCore.11")+this.frameworkProto.getName()+ //$NON-NLS-1$
                             Messages.getString("RhinoCore.12") + x, x); //$NON-NLS-1$
            }
        }

        public boolean needsUpdate() {
            return this.frameworkProto.lastCodeUpdate() > this.lastTypeInfoUpdate;
        }

        public void setParentType(TypeInfo type) {
            this.parentType = type;
            if (type == null) {
                this.objProto.setPrototype(null);
            } else {
                this.objProto.setPrototype(type.objProto);
            }
        }

        public TypeInfo getParentType() {
            return this.parentType;
        }

        public boolean hasError() {
            TypeInfo p = this;
            while (p != null) {
                if (p.error != null)
                    return true;
                p = p.parentType;
            }
            return false;
        }

        public String getError() {
            TypeInfo p = this;
            while (p != null) {
                if (p.error != null)
                    return p.error;
                p = p.parentType;
            }
            return null;
        }

        @Override
        public String toString() {
            return ("TypeInfo[" + this.frameworkProto + "," + new Date(this.lastTypeInfoUpdate) + "]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    /**
     *  Object wrapper class
     */
    class WrapMaker extends WrapFactory {

        @Override
        public Object wrap(Context cx, Scriptable scope, Object obj, Class staticType) {
            // taking a shortcut here on things normally defined by setJavaPrimitivesWrap()
            if (obj == null || obj == Undefined.instance
                    || obj instanceof Scriptable || obj instanceof String
                    || obj instanceof Number || obj instanceof Boolean) {
                return obj;
            }
            // Wrap Nodes
            if (obj instanceof NodeInterface) {
                return getNodeWrapper((NodeInterface) obj);
            }
            if (obj instanceof NodeHandle) {
                return getNodeWrapper((NodeHandle) obj);
            }

            // Masquerade SystemMap and WrappedMap as native JavaScript objects
            if (obj instanceof SystemMap || obj instanceof WrappedMap) {
                return new MapWrapper((Map) obj, RhinoCore.this);
            }

            // Convert java.util.Date objects to JavaScript Dates
            if (obj instanceof Date) {
                Object[] args = { new Long(((Date) obj).getTime()) };
                try {
                    return cx.newObject(RhinoCore.this.global, "Date", args); //$NON-NLS-1$
                 } catch (JavaScriptException nafx) {
                    return obj;
                }
            }

            // Wrap scripted Java objects
            if (RhinoCore.this.app.getPrototypeName(obj) != null) {
                return getElementWrapper(obj);
            }

            return super.wrap(cx, scope, obj, staticType);
        }

        @Override
        public Scriptable wrapNewObject(Context cx, Scriptable scope, Object obj) {
            if (obj instanceof Scriptable) {
                return (Scriptable) obj;
            }
            if (obj instanceof NodeInterface) {
                return getNodeWrapper((NodeInterface) obj);
            }

            if (obj != null && RhinoCore.this.app.getPrototypeName(obj) != null) {
                return getElementWrapper(obj);
            }

            return super.wrapNewObject(cx, scope, obj);
        }
    }

    class StringTrim extends BaseFunction {
        private static final long serialVersionUID = -1515630068911501925L;

        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            String str = thisObj.toString();
            return str.trim();
        }
    }

    class DateFormat extends BaseFunction {
        private static final long serialVersionUID = 4694440247686532087L;

        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            Date date = new Date((long) ScriptRuntime.toNumber(thisObj));
            SimpleDateFormat df;

            if (args.length > 0 && args[0] != Undefined.instance && args[0] != null) {
                if (args.length > 1 && args[1] instanceof NativeJavaObject) {
                    Object locale = ((NativeJavaObject) args[1]).unwrap();
                    if (locale instanceof Locale) {
                        df = new SimpleDateFormat(args[0].toString(), (Locale) locale);
                    } else {
                        throw new IllegalArgumentException(Messages.getString("RhinoCore.13") + //$NON-NLS-1$
                                                            locale.getClass());
                    }
                } else {
                    df = new SimpleDateFormat(args[0].toString());
                }
            } else {
                df = new SimpleDateFormat();
            }
            return df.format(date);
        }
    }

    class NumberFormat extends BaseFunction {
        private static final long serialVersionUID = -6999409297243210875L;

        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            DecimalFormat df;
            if (args.length > 0 && args[0] != Undefined.instance) {
                df = new DecimalFormat(args[0].toString());
            } else {
                df = new DecimalFormat("#,##0.00"); //$NON-NLS-1$
            }
            return df.format(ScriptRuntime.toNumber(thisObj));
        }
    }

    class HelmaContextFactory extends ContextFactory {

        final boolean strictVars = "true".equalsIgnoreCase(RhinoCore.this.app.getProperty("strictVars")); //$NON-NLS-1$ //$NON-NLS-2$

        @Override
        protected void onContextCreated(Context cx) {
            cx.setWrapFactory(RhinoCore.this.wrapper);
            cx.setOptimizationLevel(RhinoCore.this.optLevel);
            cx.setInstructionObserverThreshold(10000);
            if (Context.isValidLanguageVersion(RhinoCore.this.languageVersion)) {
                cx.setLanguageVersion(RhinoCore.this.languageVersion);
            } else {
                RhinoCore.this.app.logError(Messages.getString("RhinoCore.14") + RhinoCore.this.languageVersion); //$NON-NLS-1$
            }

            // Set up visual debugger if rhino.debug = true
            if (RhinoCore.this.hasDebugger)
                initDebugger(cx);
            super.onContextCreated(cx);
        }

        @Override
        protected boolean hasFeature(Context cx, int featureIndex) {
            switch (featureIndex) {
                case Context.FEATURE_DYNAMIC_SCOPE:
                    return true;

                case Context.FEATURE_STRICT_VARS:
                case Context.FEATURE_WARNING_AS_ERROR:
                    return this.strictVars;

                default:
                    return super.hasFeature(cx, featureIndex);
            }
        }

        /**
         * Implementation of
         * {@link Context#observeInstructionCount(int instructionCount)}.
         * This can be used to customize {@link Context} without introducing
         * additional subclasses.
         */
        @Override
        protected void observeInstructionCount(Context cx, int instructionCount) {
            RhinoEngine engine = RhinoEngine.getRhinoEngine();
            if (engine != null && engine.thread != Thread.currentThread()) {
                throw new EvaluatorException(Messages.getString("RhinoCore.15")); //$NON-NLS-1$
            }
        }
    }
}
