package com.htc.pclconverter.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.htc.pclconverter.service.AzureBlobService;

/**
 * @author kumar.charanswain
 *
 */

@RestController
public class AzureBlobFileController {

	@Autowired
	AzureBlobService azureAdapter;

	@PostMapping(path = "/upload", consumes = { MediaType.MULTIPART_FORM_DATA_VALUE })
	public String uploadFile(@RequestPart(value = "file", required = true) MultipartFile[] files) {
		return azureAdapter.upload(files);
	}

	@GetMapping("/download")
	public String uploadFile() throws IOException {
		return azureAdapter.getFile();
	}
	
	@GetMapping("/message")
	public String message(){
		return "Congrats ! your application deployed successfully in Azure Platform. !";
	}
	
}