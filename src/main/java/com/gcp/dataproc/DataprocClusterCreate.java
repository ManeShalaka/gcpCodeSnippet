package com.gcp.dataproc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.InstanceGroupConfig;
import com.google.common.collect.ImmutableList;

public class DataprocClusterCreate {

	private static final List<String> SCOPES = ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");

	public void createCluster(String projectId, String region, String clusterName, String serviceKey)
			throws IOException, InterruptedException {
		
		String endPoint = String.format("%s-dataproc.googleapis.com:443", region);
		
		CredentialsProvider credentialsProvider = getCredentials(serviceKey);
		
		// ClusterControllerService provides methods to manage clusters of ComputeEngine instances
		ClusterControllerSettings clusterControllerSettings = ClusterControllerSettings.newBuilder()
				.setEndpoint(endPoint)
				.setCredentialsProvider(credentialsProvider)
				.build();
	
		try (ClusterControllerClient clusterControllerClient = ClusterControllerClient
				.create(clusterControllerSettings)) {
			
			// master node configuration
			InstanceGroupConfig masterConfig = InstanceGroupConfig.newBuilder()
					.setMachineTypeUri("n1-standard-1")
					.setNumInstances(1)
					.build();
			
			// worker node configuration
			InstanceGroupConfig workerConfig = InstanceGroupConfig.newBuilder()
					.setMachineTypeUri("n1-standard-1")
					.setNumInstances(2)
					.build();
			
			ClusterConfig clusterConfig = ClusterConfig.newBuilder()
					.setMasterConfig(masterConfig)
					.setWorkerConfig(workerConfig)
					.build();
			
			Cluster cluster = Cluster.newBuilder()
					.setClusterName(clusterName)
					.setConfig(clusterConfig)
					.build();
			
			// creates cluster in the project
			OperationFuture<Cluster, ClusterOperationMetadata> createClusterAsyncRequest = 
					clusterControllerClient.createClusterAsync(projectId, region, cluster);
			
			// gives a response after cluster is created successfully
			Cluster response = createClusterAsyncRequest.get();
			
			System.out.println(String.format("Cluster created successfully: %s", response.getClusterName()));

		} catch (Exception e) {
			System.out.println("Error creating the cluster controller client: \n" + e.toString());
			e.printStackTrace();
		}
	}

	// Method that reads the service account key and converts to GoogleCredentials
	// base type for credentials for authorizing calls to Google APIs using OAuth2. 
	private static CredentialsProvider getCredentials(String serviceKey) throws IOException {
		String accountKey = new String(Files.readAllBytes(
		            new File(serviceKey).toPath()));
		
		GoogleCredentials credentials;
		    try (InputStream inputStream = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
		      credentials = GoogleCredentials.fromStream(inputStream).createScoped(SCOPES);
		    }
		    
		CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
		System.out.println("The credentials are : \n" + credentialsProvider.getCredentials());
		
		return credentialsProvider;
	}

	
	public static void main(String[] args) {
		DataprocClusterCreate dataproc = new DataprocClusterCreate();
		try {
			
			// use :  gcloud projects list to get projectId
			String projectId = "<<project_id>>";
			
			// region to create cluster 
			String region = "us-central1";
			
			// cluster name 
			String clusterName = "test-cluster";
			
			// service account key path (.json file)
			String serviceKey= "<<path_to_service_key>>.json";

			System.out.println("Creatig Cluster on Dataproc:");
			dataproc.createCluster(projectId, region, clusterName, serviceKey);
	
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
