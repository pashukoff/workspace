package com.javacodegeeks.snippets.enterprise.embeddedjetty;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.javacodegeeks.snippets.enterprise.embeddedjetty.ExampleServlet;

public class EmbeddedJettyMain {

	public static void main(String[] args) throws Exception {

		Server server = new Server(7070);
		ServletContextHandler handler = new ServletContextHandler(server, "/example");
		handler.addServlet(ExampleServlet.class, "/");
		server.start();

	}

}