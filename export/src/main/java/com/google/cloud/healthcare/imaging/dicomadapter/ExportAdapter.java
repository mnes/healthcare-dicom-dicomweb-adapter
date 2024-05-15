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

import com.beust.jcommander.JCommander;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.healthcare.*;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.Event;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.MonitoringService;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4che3.net.Connection;

import java.io.IOException;
import java.util.Arrays;

public class ExportAdapter {

  public static HttpRequestFactory createHttpRequestFactory(GoogleCredentials credentials) {
    if (credentials == null) {
      return new NetHttpTransport().createRequestFactory();
    }
    return new NetHttpTransport().createRequestFactory(new HttpCredentialsAdapter(credentials));
  }

  public static void main(String[] args) throws IOException {
    Flags flags = new Flags();
    JCommander jCommander = new JCommander(flags);
    jCommander.parse(args);

    // Adjust logging.
    LogUtil.Log4jToStdout(flags.verbose ? "DEBUG" : "ERROR");

    // DicomWeb client for source of DICOM.
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (!flags.oauthScopes.isEmpty()) {
      credentials = credentials.createScoped(Arrays.asList(flags.oauthScopes.split(",")));
    }

    DicomWebClient dicomWebClient =
        new DicomWebClient(createHttpRequestFactory(credentials), StringUtil.trim(flags.dicomwebAddr), "studies");

    // Initialize Monitoring
    if (!flags.monitoringProjectId.isEmpty()) {
      HttpRequestFactory monitoringRequestFactory = createHttpRequestFactory(credentials);
      MonitoringService
          .initialize(flags.monitoringProjectId, Event.values(), monitoringRequestFactory);
      MonitoringService.addEvent(Event.STARTED);
    } else {
      MonitoringService.disable();
    }

    // Use either C-STORE or STOW-RS to send DICOM, based on flags.
    boolean isStowRs = !flags.peerDicomwebAddress.isEmpty()
        || (!flags.peerDicomwebAddr.isEmpty() && !flags.peerDicomwebStowPath.isEmpty());
    boolean isCStore =
        !flags.peerDimseAET.isEmpty() && !flags.peerDimseIP.isEmpty() && flags.peerDimsePort != 0;
    DicomSender dicomSender = null;
    if (isStowRs && isCStore) {
      System.err.println("Both C-STORE and STOW-RS flags should not be specified.");
      System.exit(1);
    } else if (isStowRs) {
      // STOW-RS sender.
      boolean isLegacyAdress = flags.peerDicomwebAddress.isEmpty();
      String peerDicomwebAddress =
        isLegacyAdress ? flags.peerDicomwebAddr : flags.peerDicomwebAddress;
      String peerDicomwebStowpath = isLegacyAdress ? flags.peerDicomwebStowPath : "studies";
      IDicomWebClient exportDicomWebClient =
          new DicomWebClientJetty(flags.useGcpApplicationDefaultCredentials ? null : credentials,
              StringUtil.joinPath(peerDicomwebAddress, peerDicomwebStowpath), false);
      dicomSender =
          new StowRsSender(dicomWebClient, exportDicomWebClient);
      System.out.printf(
          "Export adapter set-up to export via STOW-RS to address: %s, path: %s\n",
          peerDicomwebAddress, peerDicomwebStowpath);
    } else if (isCStore) {
      // C-Store sender.
      //
      // DIMSE application entity.
      ApplicationEntity applicationEntity = new ApplicationEntity("EXPORTADAPTER");
      Connection conn = new Connection();
      DeviceUtil.createClientDevice(applicationEntity, conn);
      applicationEntity.addConnection(conn);
      dicomSender =
          new CStoreSender(
              applicationEntity,
              flags.peerDimseAET,
              flags.peerDimseIP,
              flags.peerDimsePort,
              dicomWebClient);
      System.out.printf(
          "Export adapter set-up to export via C-STORE to AET: %s, IP: %s, Port: %d\n",
          flags.peerDimseAET, flags.peerDimseIP, flags.peerDimsePort);
    } else {
      System.err.println("Neither C-STORE nor STOW-RS flags have been specified.");
      System.exit(1);
    }
    try {
      MonitoringService.addEvent(Event.REQUEST);
      Thread.sleep(5000);
      if (!flags.messageData.isEmpty()) {
        // Create a ByteString from a string message
        ByteString byteString = ByteString.copyFromUtf8(flags.messageData);
        // Create a new PubsubMessage object
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(byteString) // Set the message data
                .build();
        dicomSender.send(message);
        System.out.println("Message Data:" + message.getData().toStringUtf8());
      } else {
        System.err.println("no messageData flags have been specified.");
        System.exit(1);
      }
    } catch (Exception e) {
      MonitoringService.addEvent(Event.ERROR);
      e.printStackTrace();
    }
  }
}
