package com.db.dataplatform.techtest.client.component;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

public interface Client {
    void pushData(DataEnvelope dataEnvelope) throws JsonProcessingException;
    List<DataEnvelope> getData(String blockType);
    boolean updateData(String blockName, String newBlockType) throws UnsupportedEncodingException;

    List<DataEnvelope> getDataByBlockType(String blockType);

    boolean updateBlockType(String blockName, String newBlockType);

}
