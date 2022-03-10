package com.htc.pclconverter.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

@Component
public class PdfToPclAzureScheduler {

	@Autowired
	BlobClientBuilder client;

	@Value("${blob.connection-string}")
	private String storageConnectionString;

	@Value("${blob.dest.connection-string}")
	private String destStorageConnectionString;

	@Value("${blob.connection.accontName.accountKey}")
	private String connectionNameKey;

	@Value("${blob.container.name}")
	private String containerName;

	@Value("${blob.backup.container.name}")
	private String backupContainerName;

	@Value("${file.root.location}")
	private String fileRootLocation;

	@Value("${file.output.location}")
	private String fileOutputLocation;

	@Scheduled(fixedDelay = 40000)
	public void pdfToPclConversionScheduler() {

		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionNameKey)
				.buildClient();
		BlobServiceClient destblobServiceClient = new BlobServiceClientBuilder()
				.connectionString(destStorageConnectionString).buildClient();
		BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
		BlobContainerClient destcontainer = destblobServiceClient.getBlobContainerClient(backupContainerName);
		try {
			CloudStorageAccount account = CloudStorageAccount.parse(connectionNameKey);
			CloudBlobClient serviceClient = account.createCloudBlobClient();
			File targetFile = new File(fileRootLocation);
			if (!targetFile.exists()) {
				targetFile.mkdir();
			}
			int pageCount = 0;
			ConcurrentHashMap<Integer, List<String>> map = new ConcurrentHashMap<Integer, List<String>>();
			CloudBlobContainer container = serviceClient.getContainerReference(containerName);

			container.createIfNotExists();
			Iterable<ListBlobItem> blobs = container.listBlobs();
			for (ListBlobItem blob : blobs) {
				String[] str1 = blob.getUri().toString().split("/");
				File updateFile = new File(targetFile.getPath() + "\\" + str1[4].replace("%20", " "));
				String fileExtension = FilenameUtils.getExtension(updateFile.toString());
				if (fileExtension.equals("pcl"))
					continue;
				CloudBlockBlob cloudBlob = (CloudBlockBlob) blob;
				cloudBlob.downloadToFile(updateFile.toString());
				PDDocument doc = PDDocument.load(new File(updateFile.getPath()));
				pageCount = doc.getPages().getCount();
				if (map.containsKey(pageCount)) {
					List<String> existingFileNameList = new ArrayList<String>();
					existingFileNameList = map.get(doc.getPages().getCount());
					existingFileNameList.add(updateFile.getName());
					map.put(pageCount, existingFileNameList);
				} else {
					List<String> existingFileNameList = new ArrayList<String>();
					existingFileNameList.add(updateFile.getName());
					map.put(pageCount, existingFileNameList);
				}

				BlobClient blobClient = containerClient.getBlobClient(str1[4].replace("%20", " "));
				BlobClient destblobclient = destcontainer.getBlobClient(str1[4].replace("%20", " "));
				destblobclient.beginCopy(blobClient.getBlobUrl(), null);
				cloudBlob.delete();
				doc.close();
			}

			map.forEach((key, value) -> {

				for (String temp : value) {
					if (temp.contains("Banner")) {
						String bannerPage = temp;
						value.remove(temp);
						value.add(0, bannerPage);
						break;
					}
				}

			});
			System.out.println("update Map:" + map);
			PDFMergerUtility PDFmerger = new PDFMergerUtility();
			map.forEach((key, fileNames) -> {
				for (String sortPdfFile : fileNames) {
					try {
						PDFmerger.addSource(fileRootLocation + "\\" + sortPdfFile);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
				}
				try {
					String fileNoExt = fileNames.get(1).replaceFirst("[.][^.]+$", "");
					PDFmerger.setDestinationFileName(fileRootLocation + "\\" + fileNoExt + "-mgr" + ".pdf");
					PDFmerger.mergeDocuments();
					File pdfMergePclFile = new File(fileOutputLocation);
					if (!pdfMergePclFile.exists()) {
						pdfMergePclFile.mkdir();
					}
					String outputFile = fileOutputLocation + fileNoExt + "-mgr" + ".pcl";

					pdfMergePclFile = new File(outputFile);
					PDDocument document = new PDDocument();
					document.save(outputFile);

					FileInputStream input = new FileInputStream(pdfMergePclFile);
					client.blobName(pdfMergePclFile.getName()).buildClient().upload(input, pdfMergePclFile.length());
					input.close();
					document.close();
				} catch (Exception exception) {
					exception.printStackTrace();
				}
			});

			FileUtils.deleteDirectory(targetFile);
		} catch (Exception exception) {
			if (exception.getMessage().contains("BlobAlreadyExists")) {
				System.out.println("The specified document already exists" + exception.getMessage());
				System.out.println("Exception:" + exception.getMessage());
			} else {
				System.out.println("Exception Message:" + exception);
				System.out.println("error in merge pdf and covert into pcl file successfully" + exception.getMessage());
				System.out.println("Exception:" + exception.getMessage() + exception.getMessage());
			}
		}

	}

}
