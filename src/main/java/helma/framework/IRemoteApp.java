package helma.framework;

/*
 * #%L
 * HelmaObjectPublisher
 * %%
 * Copyright (C) 1998 - 2012 Helma Software
 * %%
 * Helma License Notice
 * 
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is available at
 * http://adele.helma.org/download/helma/license.txt
 * #L%
 */

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI interface for an application. Currently only execute is used and
 * supported.
 */
public interface IRemoteApp extends Remote {
	/**
	 * 
	 * 
	 * @param param
	 *            ...
	 * 
	 * @return ...
	 * 
	 * @throws RemoteException
	 *             ...
	 */
	public ResponseTrans execute(RequestTrans param) throws RemoteException;

	/**
	 * 
	 * 
	 * @throws RemoteException
	 *             ...
	 */
	public void ping() throws RemoteException;
}
