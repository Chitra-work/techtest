package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) {

        // Calculate MD5 checksum
        String calculatedChecksum = calculateMD5Checksum(envelope.getDataBody().getDataBody());

        // Compare with client-provided checksum
        if (envelope.getChecksum().equals(calculatedChecksum)) {
            // Save to persistence if checksums match
            persist(envelope);
            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
            return true;
        } else {
            // Log an error or handle invalid data
            log.error("Checksum mismatch for data name: {}", envelope.getDataHeader().getName());
            return false;
        }
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    private String calculateMD5Checksum(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data.getBytes());
            byte[] digest = md.digest();

            // Convert the byte array to a hexadecimal string
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // Handle the exception or log an error
            log.error("MD5 algorithm not available", e);
            return null;  // or throw a RuntimeException
        }
    }


    @Override
    public List<DataEnvelope> getDataByBlockType(String blockType) {
        log.info("Querying for data with header block type {}", blockType);

        // Implement the logic to fetch data by block type from the persistence store
        List<DataBodyEntity> dataEntities = dataBodyServiceImpl.getDataByBlockType(BlockTypeEnum.valueOf(blockType));

        // Convert data entities to data envelopes
        List<DataEnvelope> dataEnvelopes = dataEntities.stream()
                .map(this::convertToDataEnvelope)
                .collect(Collectors.toList());

        log.info("Query successful. Found {} data envelopes with block type {}", dataEnvelopes.size(), blockType);

        return dataEnvelopes;
    }

    private DataEnvelope convertToDataEnvelope(DataBodyEntity dataBodyEntity) {
        DataEnvelope dataEnvelope = new DataEnvelope();
        // Map DataHeaderEntity properties
        DataHeaderEntity dataHeaderEntity = dataBodyEntity.getDataHeaderEntity();
        DataHeader dataHeader = modelMapper.map(dataHeaderEntity, DataHeader.class);
        dataEnvelope.setDataHeader(dataHeader);

        // Map DataBodyEntity properties
        DataBody dataBody = modelMapper.map(dataBodyEntity, DataBody.class);
        dataEnvelope.setDataBody(dataBody);

        return dataEnvelope; // Map DataHeaderEntity properties

    }

    @Override
    public boolean updateBlockType(String blockName, String newBlockType) {
        // Implement logic to update the block type in the persistence store
        // Validate input parameters (blockName)
        if (blockName == null || blockName.isEmpty()) {
            log.error("Invalid blockName provided for block type update");
            return false;
        }

        // Implement logic to update block type in the persistence store
        boolean updateSuccess = dataBodyServiceImpl.updateBlockType(blockName, newBlockType);

        if (updateSuccess) {
            log.info("Block type updated successfully for block with name {}", blockName);
            return true;
        } else {
            log.error("Failed to update block type for block with name {}", blockName);
            return false;
        }
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());

        try {
            // Save to persistence
            DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);
            DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
            dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);
            saveData(dataBodyEntity);

            // Push to Hadoop data lake
            pushToHadoopDataLake(envelope);

        } catch (Exception e) {
            log.error("Error while persisting data", e);
        }
    }

    private void pushToHadoopDataLake(DataEnvelope envelope) {
        log.info("Pushing data to Hadoop data lake for attribute name: {}", envelope.getDataHeader().getName());

        // Create HttpHeaders with Content-Type as JSON
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Create an HttpEntity with the dataEnvelope and headers
        HttpEntity<DataEnvelope> requestEntity = new HttpEntity<>(envelope, headers);

        // Create a RestTemplate to make the HTTP request
        RestTemplate restTemplate = new RestTemplate();

        try {
            // Send a POST request to the Hadoop data lake and receive the response as a ResponseEntity
            ResponseEntity<Void> responseEntity = restTemplate.postForEntity(
                    "http://localhost:8090/hadoopserver/pushbigdata",
                    requestEntity,
                    Void.class
            );

            // Check if the request was successful (HTTP status code 2xx)
            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                log.info("Push to Hadoop data lake successful.");
            } else {
                log.error("Failed to push data to Hadoop data lake. Server returned status code: {}", responseEntity.getStatusCodeValue());
            }

        } catch (Exception e) {
            log.error("Error while pushing data to Hadoop data lake", e);
        }
    }





}
