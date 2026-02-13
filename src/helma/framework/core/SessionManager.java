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

package helma.framework.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import helma.objectmodel.NodeInterface;
import helma.objectmodel.db.NodeHandle;
import helma.objectmodel.db.Transactor;
import helma.scripting.ScriptingEngineInterface;

public class SessionManager {

    protected Hashtable sessions;

    protected Application app;

    public SessionManager() {
        this.sessions = new Hashtable();
    }

    public void init(Application app) {
        this.app = app;
    }

    public void shutdown() {
        this.sessions.clear();
    }

    public Session createSession(String sessionId) {
        Session session = getSession(sessionId);
        if (session == null) {
            session = new Session(sessionId, this.app);
        }
        return session;
    }

    public Session getSession(String sessionId) {
        if (sessionId == null) {
            return null;
        }
        return (Session) this.sessions.get(sessionId);
    }

    public void registerSession(Session session) {
        this.sessions.put(session.getSessionId(), session);        
    }

    /**
     *  Return the whole session map. We return a clone of the table to prevent
     * actual changes from the table itself, which is managed by the application.
     * It is safe and allowed to manipulate the session objects contained in the table, though.
     */
    public Map getSessions() {
        return (Map) this.sessions.clone();
    }

    /**
     * Returns the number of currenty active sessions.
     */
    public int countSessions() {
        return this.sessions.size();
    }

    /**
     * Remove the session from the sessions-table and logout the user.
     */
    public void discardSession(Session session) {
        session.logout();
        this.sessions.remove(session.getSessionId());
    }


    /**
     * Return an array of <code>SessionBean</code> objects currently associated with a given
     * Helma user.
     */
    public List getSessionsForUsername(String username) {
        ArrayList list = new ArrayList();

        if (username == null) {
            return list;
        }

        Enumeration e = this.sessions.elements();
        while (e.hasMoreElements()) {
            Session s = (Session) e.nextElement();

            if (s != null && username.equals(s.getUID())) {
                // append to list if session is logged in and fits the given username
                list.add(new SessionBean(s));
            }
        }

        return list;
    }

    /**
     * Return a list of Helma nodes (HopObjects -  the database object representing the user,
     *  not the session object) representing currently logged in users.
     */
    public List getActiveUsers() {
        ArrayList list = new ArrayList();

        for (Enumeration e = this.sessions.elements(); e.hasMoreElements();) {
            Session s = (Session) e.nextElement();

            if (s != null && s.isLoggedIn()) {
                // returns a session if it is logged in and has not been
                // returned before (so for each logged-in user is only added once)
                NodeInterface node = s.getUserNode();

                // we check again because user may have been logged out between the first check
                if (node != null && !list.contains(node)) {
                    list.add(node);
                }
            }
        }

        return list;
    }


    /**
     * Dump session state to a file.
     *
     * @param f the file to write session into, or null to use the default sesssion store.
     */
    public void storeSessionData(File f, ScriptingEngineInterface engine) {
        if (f == null) {
            f = new File(this.app.dbDir, "sessions"); //$NON-NLS-1$
        }

        try {
            OutputStream ostream = new BufferedOutputStream(new FileOutputStream(f));
            ObjectOutputStream p = new ObjectOutputStream(ostream);

            synchronized (this.sessions) {
                p.writeInt(this.sessions.size());

                for (Enumeration e = this.sessions.elements(); e.hasMoreElements();) {
                    try {
                        engine.serialize(e.nextElement(), p);
                        // p.writeObject(e.nextElement());
                    } catch (NotSerializableException nsx) {
                        // not serializable, skip this session
                        this.app.logError(Messages.getString("SessionManager.0"), nsx); //$NON-NLS-1$
                    }
                }
            }

            p.flush();
            ostream.close();
            this.app.logEvent(Messages.getString("SessionManager.1") + this.sessions.size() + Messages.getString("SessionManager.2")); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (Exception e) {
            this.app.logError(Messages.getString("SessionManager.3"), e); //$NON-NLS-1$
        }
    }

    /**
     * loads the serialized session table from a given file or from dbdir/sessions
     */
    public void loadSessionData(File f, ScriptingEngineInterface engine) {
        if (f == null) {
            f = new File(this.app.dbDir, "sessions"); //$NON-NLS-1$
        }
        
        // if session file doesn't exist or is empty, nothing to load
        if (!f.exists() || f.length() == 0) {
            return;
        }

        // compute session timeout value
        int sessionTimeout = 30;

        try {
            sessionTimeout = Math.max(0,
                                      Integer.parseInt(this.app.getProperty("sessionTimeout", //$NON-NLS-1$
                                                                         "30"))); //$NON-NLS-1$
        } catch (Exception ignore) {
            System.out.println(ignore.toString());
        }

        long now = System.currentTimeMillis();
        Transactor tx = Transactor.getInstance(this.app.getNodeManager());

        try {
            tx.begin("sessionloader"); //$NON-NLS-1$
            // load the stored data:
            InputStream istream = new BufferedInputStream(new FileInputStream(f));
            ObjectInputStream p = new ObjectInputStream(istream);
            int size = p.readInt();
            int ct = 0;
            Hashtable newSessions = new Hashtable();

            while (ct < size) {
                Session session = (Session) engine.deserialize(p);

                if ((now - session.lastTouched()) < (sessionTimeout * 60000)) {
                    session.setApp(this.app);
                    newSessions.put(session.getSessionId(), session);
                }

                ct++;
            }

            p.close();
            istream.close();
            this.sessions = newSessions;
            this.app.logEvent(Messages.getString("SessionManager.4") + newSessions.size() + Messages.getString("SessionManager.5")); //$NON-NLS-1$ //$NON-NLS-2$
            tx.commit();
        } catch (FileNotFoundException fnf) {
            // suppress error message if session file doesn't exist
            tx.abort();
        } catch (java.io.EOFException eof) {
            // empty or truncated session file -> ignore and continue
            tx.abort();
        } catch (Exception e) {
            this.app.logError(Messages.getString("SessionManager.6"), e); //$NON-NLS-1$
            tx.abort();
        } finally {
            tx.closeConnections();
        }
    }

    /**
     * Purge sessions that have not been used for a certain amount of time.
     * This is called by run().
     *
     * @param lastSessionCleanup the last time sessions were purged
     * @return the updated lastSessionCleanup value
     */
    protected long cleanupSessions(long lastSessionCleanup) {

        long now = System.currentTimeMillis();
        long sessionCleanupInterval = 60000;

        // check if we should clean up user sessions
        if ((now - lastSessionCleanup) > sessionCleanupInterval) {

            // get session timeout
            int sessionTimeout = 30;

            try {
                sessionTimeout = Math.max(0,
                        Integer.parseInt(this.app.getProperty("sessionTimeout", "30"))); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (NumberFormatException nfe) {
                this.app.logEvent(Messages.getString("SessionManager.7") + this.app.getProperty(Messages.getString("SessionManager.8"))); //$NON-NLS-1$ //$NON-NLS-2$
            }

            RequestEvaluator thisEvaluator = null;

            try {

                thisEvaluator = this.app.getEvaluator();

                Session[] sessionArray = (Session[]) this.sessions.values().toArray(new Session[0]);

                for (int i = 0; i < sessionArray.length; i++) {
                    Session session = sessionArray[i];

                    session.pruneUploads();
                    if ((now - session.lastTouched()) > (sessionTimeout * 60000)) {
                        NodeHandle userhandle = session.userHandle;

                        if (userhandle != null) {
                            try {
                                Object[] param = {session.getSessionId()};

                                thisEvaluator.invokeInternal(userhandle, "onLogout", param); //$NON-NLS-1$
                            } catch (Exception x) {
                                // errors should already be logged by requestevaluator, but you never know
                                this.app.logError(Messages.getString("SessionManager.9"), x); //$NON-NLS-1$
                            }
                        }

                        discardSession(session);
                    }
                }
            } catch (Exception cx) {
                this.app.logError(Messages.getString("SessionManager.10"), cx); //$NON-NLS-1$
            } finally {
                if (thisEvaluator != null) {
                    this.app.releaseEvaluator(thisEvaluator);
                }
            }
            return now;
        }
        return lastSessionCleanup;
    }


}
