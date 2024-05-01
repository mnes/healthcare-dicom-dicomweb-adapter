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

import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.Event;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.MonitoringService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportMessageReceiver implements MessageReceiver {
  private DicomSender dicomSender;
  private static final Logger log = LoggerFactory.getLogger(ExportMessageReceiver.class);

  ExportMessageReceiver(DicomSender dicomSender) {
    this.dicomSender = dicomSender;
  }

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    try {
      MonitoringService.addEvent(Event.REQUEST);
      dicomSender.send(message);
      log.info("message: "+message);
      log.info("message.getMessageId(): "+message.getMessageId());
      log.info("message.getAttributesMap(): "+message.getAttributesMap());
      log.info("message.getData(): "+message.getData().toString());

      consumer.ack();
    } catch (Exception e) {
      MonitoringService.addEvent(Event.ERROR);
      e.printStackTrace();
      consumer.nack();
    }
  }
}
