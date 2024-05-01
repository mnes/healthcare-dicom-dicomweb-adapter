// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.healthcare.imaging.dicomadapter;

import com.google.cloud.healthcare.DicomWebClient;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.Event;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.MonitoringService;
import com.google.common.io.CountingInputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.PubsubMessage;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.dcm4che3.data.Tag;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4che3.util.TagUtils;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CStoreSender sends DICOM to peer using DIMSE C-STORE protocol.
public class CStoreSender implements DicomSender {
  private static final Logger log = LoggerFactory.getLogger(CStoreSender.class);
  private static final String SOP_CLASS_UID_TAG = TagUtils.toHexString(Tag.SOPClassUID);
  private static final String SOP_INSTANCE_UID_TAG = TagUtils.toHexString(Tag.SOPInstanceUID);
  private final ApplicationEntity applicationEntity;
  private final String dimsePeerAET;
  private final String dimsePeerIP;
  private final int dimsePeerPort;
  private final DicomWebClient dicomWebClient;

  CStoreSender(
      ApplicationEntity applicationEntity,
      String dimsePeerAET,
      String dimsePeerIP,
      int dimsePeerPort,
      DicomWebClient dicomWebClient) {
    this.applicationEntity = applicationEntity;
    this.dimsePeerAET = dimsePeerAET;
    this.dimsePeerIP = dimsePeerIP;
    this.dimsePeerPort = dimsePeerPort;
    this.dicomWebClient = dicomWebClient;
  }


  private int UniqueIntegerGenerator() {
    try {
      // Generate a UUID
      UUID uuid = UUID.randomUUID();

      // Convert UUID to integer using hashCode() and bitwise operations
      int hash = uuid.hashCode();
      int unsignedInt16 = hash & 0xFFFF; // Mask with 16-bit unsigned integer

      // Print the generated 16-bit unsigned integer
      log.info("UUID(MessageID): " + uuid);
      log.info("16-bit Unsigned Integer(MessageID): " + unsignedInt16);

      return unsignedInt16;
    } catch (Exception e) {
      e.printStackTrace();
      return 0;
    }
  }

  @Override
  public void send(PubsubMessage message) throws Exception {
    String wadoUri = message.getData().toStringUtf8();
    String qidoUri = qidoFromWadoUri(wadoUri);

    // Invoke QIDO-RS to get DICOM tags needed to invoke C-Store.
    JSONArray qidoResponse = dicomWebClient.qidoRs(qidoUri);
    if (qidoResponse.length() != 1) {
      throw new IllegalArgumentException(
          "Invalid QidoRS JSON array length for response: " + qidoResponse.toString());
    }
    String sopClassUid = AttributesUtil.getTagValue(qidoResponse.getJSONObject(0),
        SOP_CLASS_UID_TAG);
    String sopInstanceUid = AttributesUtil.getTagValue(qidoResponse.getJSONObject(0),
        SOP_INSTANCE_UID_TAG);

    // START(create file using UUID for every pubsub request
    // for destination dataset and dataStore location)
    int MessageID = UniqueIntegerGenerator();
    Map<String, String> fileDataMap = new HashMap<>();
    fileDataMap.putAll(message.getAttributesMap());
    fileDataMap.put("pubsubMessageId",message.getMessageId());
    fileDataMap.put("data",message.getData().toString("UTF-8"));
    log.info("fileDataMap: "+fileDataMap);

    // Convert Java object to JSON string
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String jsonString = gson.toJson(fileDataMap);

    // Write JSON string to file
    try (FileWriter writer = new FileWriter(System.getenv("SHARED_FOLDER")+MessageID+".json")) {
      writer.write(jsonString);
      log.info(MessageID+".json file created successfully.");
    } catch (IOException e) {
      e.printStackTrace();
    }
    // END

    // Invoke WADO-RS to get bulk DICOM.
    InputStream responseStream = dicomWebClient.wadoRs(wadoUri);

    CountingInputStream countingStream = new CountingInputStream(responseStream);
    DicomClient.connectAndCstore(MessageID, sopClassUid, sopInstanceUid, countingStream,
        applicationEntity, dimsePeerAET, dimsePeerIP, dimsePeerPort);
    MonitoringService.addEvent(Event.BYTES, countingStream.getCount());
  }

  // Derives a QIDO-RS path using a WADO-RS path.
  // TODO(b/72555677): Find an easier way to do this instead of string manipulation.
  private String qidoFromWadoUri(String wadoUri) {
    Path wadoPath = Paths.get(wadoUri);
    String instanceUid = wadoPath.getFileName().toString();
    Path wadoParentPath = wadoPath.getParent();
    String qidoUri =
        String.format(
            "%s?%s=%s",
            wadoParentPath.toString(), SOP_INSTANCE_UID_TAG, instanceUid);
    return qidoUri;
  }
}
