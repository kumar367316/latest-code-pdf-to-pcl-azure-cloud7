package com.htc.pclconverter.exception;

/**
 * @author kumar.charanswain
 *
 */

public class ApplicationException extends RuntimeException{
	
	private static final long serialVersionUID = 1L;
	
	private String message;
	private int code;
	public ApplicationException(String message, int code) {
		super();
		this.message = message;
		this.code = code;
	}

}
