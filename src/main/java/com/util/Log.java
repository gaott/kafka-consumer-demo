package com.util;

import org.apache.commons.logging.LogFactory;

/**
 * 
 * 辅助logging类，可以简化程序中日志的调用：
 * 
 *
 */
public class Log {

	private static org.apache.commons.logging.Log logger = LogFactory.getLog("Unique");

	// error

	public static void e(String m, Throwable s) {
		logger.error(m, s);
	}

	public static void e(String m) {
		logger.error(m);
	}

	public static void e(Throwable s) {
		logger.error("", s);
	}

	// info

	public static void i(String m, Throwable s) {
		logger.info(m, s);
	}

	public static void i(String m) {
		logger.info(m);
	}

	public static void i(Throwable s) {
		logger.info("", s);
	}

	// debug

	public static void d(String m, Throwable s) {
		logger.debug(m, s);
	}

	public static void d(String m) {
		logger.debug(m);
	}

	public static void d(Throwable s) {
		logger.debug("", s);
	}

	// warn

	public static void w(String m, Throwable s) {
		logger.warn(m, s);
	}

	public static void w(String m) {
		logger.warn(m);
	}

	public static void w(Throwable s) {
		logger.warn("", s);
	}

	// fatal

	public static void f(String m, Throwable s) {
		logger.fatal(m, s);
	}

	public static void f(String m) {
		logger.fatal(m);
	}

	public static void f(Throwable s) {
		logger.fatal("", s);
	}

}
