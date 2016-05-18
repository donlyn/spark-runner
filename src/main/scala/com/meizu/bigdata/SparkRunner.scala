package com.meizu.bigdata

import java.io.File
import java.lang.reflect.Modifier
import java.net.{URISyntaxException, URI, URLClassLoader, URL}

private class MutableURLClassLoader(urls: Array[URL], parent: ClassLoader)
    extends URLClassLoader(urls, parent) {

    override def addURL(url: URL): Unit = {
        super.addURL(url)
    }

    override def getURLs: Array[URL] = {
        super.getURLs
    }
}

object SparkRunner {
    private val printStream = System.err

    def resolveURI(path: String): URI = {
        try {
            val uri = new URI(path)
            if (uri.getScheme != null) {
                return uri
            }
            // make sure to handle if the path has a fragment (applies to yarn
            // distributed cache)
            if (uri.getFragment != null) {
                val absoluteURI = new File(uri.getPath).getAbsoluteFile.toURI
                return new URI(absoluteURI.getScheme, absoluteURI.getHost, absoluteURI.getPath,
                    uri.getFragment)
            }
        } catch {
            case e: URISyntaxException =>
        }
        new File(path).getAbsoluteFile.toURI
    }

    private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
        val uri = resolveURI(localJar)
        uri.getScheme match {
            case "file" | "local" =>
                val file = new File(uri.getPath)
                if (file.exists()) {
                    loader.addURL(file.toURI.toURL)
                } else {
                    printStream.println("Warning : " + s"Local jar $file does not exist, skipping.")
                }
            case _ =>
                printStream.println("Warning : " + s"Skip remote jar $uri.")
        }
    }

    def runMain(
        childArgs: Seq[String],
        childClasspath: Seq[String],
        sysProps: Map[String, String],
        childMainClass: String,
        verbose: Boolean = false) : Unit = {
        if (verbose) {
            printStream.println(s"Main class:\n$childMainClass")
            printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
            printStream.println(s"System properties:\n${sysProps.mkString("\n")}")
            printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
            printStream.println("\n")
        }

        val loader =  new MutableURLClassLoader(new Array[URL](0), Thread.currentThread.getContextClassLoader)
        Thread.currentThread.setContextClassLoader(loader)

        for (jar <- childClasspath) {
            addJarToClasspath(jar, loader)
        }

        for ((key, value) <- sysProps) {
            System.setProperty(key, value)
        }

        var mainClass: Class[_] = Class.forName(childMainClass, true, Thread.currentThread().getContextClassLoader)
        val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
        if (!Modifier.isStatic(mainMethod.getModifiers)) {
            throw new IllegalStateException("The main method in the given main class must be static")
        }

        mainMethod.invoke(null, childArgs.toArray)
    }

    def main(args: Array[String]) {
        val childArgs = Seq(10.toString)
        val childClasspath = Seq("file:/opt/spark/lib/spark-examples-1.5.2-hadoop2.4.0.jar")
        val sysProps = Map(
            "spark.yarn.historyServer.address" -> "mz-hadoop-1.meizu.com:18090",
            "spark.eventLog.enabled"           -> "true",
            "spark.eventLog.compress"          -> "false",
            "spark.history.ui.port"            -> "18090",
            "SPARK_SUBMIT"                     -> "true",
            "spark.app.name"                   -> "org.apache.spark.examples.SparkPi",
            "spark.history.fs.logDirectory"    -> "hdfs://root/user/spark/history",
            "spark.jars"                       -> "file:/opt/spark/lib/spark-examples-1.5.2-hadoop2.4.0.jar",
            "spark.submit.deployMode"          -> "client",
            "spark.eventLog.dir"               -> "hdfs://root/user/spark/history",
            "spark.driver.maxResultSize"       -> "2g",
            "spark.master"                     -> "local[*]"
        )
        val childMainClass = "org.apache.spark.examples.SparkPi"
        runMain(childArgs, childClasspath, sysProps, childMainClass, verbose = true)
    }
}