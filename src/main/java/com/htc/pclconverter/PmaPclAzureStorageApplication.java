package com.htc.pclconverter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author kumar.charanswain
 *
 */
@SpringBootApplication
@EnableScheduling
public class PmaPclAzureStorageApplication {

	public static void main(String[] args) {
		SpringApplication.run(PmaPclAzureStorageApplication.class, args);
	}

}
