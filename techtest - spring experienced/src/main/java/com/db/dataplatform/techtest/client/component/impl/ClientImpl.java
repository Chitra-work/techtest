package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.Collections;
import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);

        // Create HttpHeaders with Content-Type as JSON
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Create an HttpEntity with the dataEnvelope and headers
        HttpEntity<DataEnvelope> requestEntity = new HttpEntity<>(dataEnvelope, headers);

        // Create a RestTemplate to make the HTTP request
        RestTemplate restTemplate = new RestTemplate();

        try {
            // Send a POST request to the server and receive the response as a ResponseEntity
            ResponseEntity<Boolean> responseEntity = restTemplate.postForEntity(URI_PUSHDATA, requestEntity, Boolean.class);

            // Check if the request was successful (HTTP status code 2xx)
            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                Boolean response = responseEntity.getBody();
                log.info("Push successful. Server response: {}", response);
            } else {
                log.error("Failed to push data. Server returned status code: {}", responseEntity.getStatusCodeValue());
            }

        } catch (Exception e) {
            log.error("Error while pushing data to the server", e);
        }
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        return null;
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
        return true;
    }

    @Override
    public List<DataEnvelope> getDataByBlockType(String blockType) {
        log.info("Querying for data with header block type {}", blockType);

        // Create a RestTemplate to make the HTTP request
        RestTemplate restTemplate = new RestTemplate();

        // Send a GET request to the server and receive the response as a ResponseEntity
        ResponseEntity<List<DataEnvelope>> responseEntity = restTemplate.exchange(
                URI_GETDATA.expand(blockType),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<DataEnvelope>>() {}
        );

        // Check if the request was successful (HTTP status code 2xx)
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            List<DataEnvelope> dataEnvelopes = responseEntity.getBody();
            log.info("Query successful. Found {} data envelopes with block type {}", dataEnvelopes.size(), blockType);
            return dataEnvelopes;
        } else {
            log.error("Failed to fetch data. Server returned status code: {}", responseEntity.getStatusCodeValue());
            return Collections.emptyList();
        }
    }

    @Override
    public boolean updateBlockType(String blockName, String newBlockType) {
        log.info("Updating block type to {} for block with name {}", newBlockType, blockName);

        // Create HttpHeaders with Content-Type as JSON
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Create an HttpEntity with the block name and new block type
        HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

        // Create a RestTemplate to make the HTTP request
        RestTemplate restTemplate = new RestTemplate();

        // Send a PATCH request to the server and receive the response as a ResponseEntity
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(
                URI_PATCHDATA.expand(blockName, newBlockType),
                HttpMethod.PATCH,
                requestEntity,
                Boolean.class
        );

        // Check if the request was successful (HTTP status code 2xx)
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            Boolean response = responseEntity.getBody();
            log.info("Block type update successful. Server response: {}", response);
            return response != null && response;
        } else {
            log.error("Failed to update block type. Server returned status code: {}", responseEntity.getStatusCodeValue());
            return false;
        }
    }


}
