This is the README file for version 1.7.3 of the Helma Javascript Web
Application Framework.

==============
STARTING HELMA
==============

* make sure you have Java 1.7 or higher as well as Apache Ant installed
* clone this repository
* build Helma with `ant jar`
* invoke `start.sh`, resp. `start.bat`, depending on your platform
* direct your web browser to http://localhost:8080

===========
ABOUT HELMA
===========

Helma is an open source web application framework for fast and efficient
scripting and serving of your websites and Internet applications.

Helma is written in Java and employs Javascript for its server-side scripting
environment, removing the need for compilation cycles and reducing development
costs while giving you instant access to leverage the whole wealth of Java
libraries out there.

Helma pioneered the simple and codeless mapping of application objects to
database tables, which has only recently come into vogue with other web
frameworks. In addition, an embedded object-oriented database performs
automatic data persistence of unmapped objects.

Helma has proven itself to be stable and fast, capable of serving high traffic
sites with hundreds of thousands of dynamic pages per day. The Austrian
Broadcasting Corporation, popular weblog hosting sites such as antville.org,
twoday.net, and blogger.de, among many others, have successfully been deploying
Helma for several years now.

===================
SYSTEM REQUIREMENTS
===================

You need a Java virtual machine 1.7 or higher to run Helma.

For Windows, Linux and Solaris you can get a Java runtime or development kit
from http://java.com/en/download/. If you are on Mac OS X, you already have a
Java runtime that will work well with Helma.

For other operating systems, please consult the documentation about the
availabilty of a Java 1.7 (or higher) runtime.

============================
INSTALLING AND RUNNING HELMA
============================

Clone this repository to your machine and start the build process with
`ant jar`.

After compilation start Helma by invoking `start.bat` or `start.sh`, depending
on whether you are on Windows or Linux / Unix / OS X. If the java command is
not found, try setting the `JAVA_HOME` variable in the start script to the
location of your Java installation.

You may also want to have a look at the start script for other settings. You
can adjust server wide settings in the `server.properties` file. For example,
you should set the `smtp` property to the name of the SMTP server that Helma
should use to send e-mail. Applications can be started or stopped by editing
the `apps.properties` file through the web interface using the management
application that is part of Helma.

After startup you should be able to connect your browser to
http://localhost:8080 – port 8080 on the local machine, that is.

Helma comes with a version of Jetty (http://eclipse.org/jetty/), a lightweight
yet industrial strenth web server developed by The Eclipse Foundation.

While Jetty works well for deploying real web sites, you may want to run Helma
behind an existing web server.
Helma can be plugged into Servlet containers using Servlet classes
that communicate with Helma either directly or via Java RMI.

=====================================
DOCUMENTATION AND FURTHER INFORMATION
=====================================

After installing and running Helma, you will be able to access introductions to
the features of Helma and the various included development tools. Further
information you will find on the helma.org website:

Guide: http://helma.org/docs/guide/
API Reference: http://helma.org/docs/reference/
Tutorial: http://helma.org/docs/tutorial/
DocBok: http://helma.org/docs/docbook/

========================
MAILING LIST AND SUPPORT
========================

Please join us on the Helma mailing lists where we will be happy to
answer any further questions you may have!

http://helma.org/development/mailinglists/
