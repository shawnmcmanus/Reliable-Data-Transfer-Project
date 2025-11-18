# SOURCES:
# https://canvas.oregonstate.edu/courses/2038813/pages/programming-project-primer-reliable-data-transmission-rdt-2?module_item_id=26026906
# https://www.reddit.com/r/OSUOnlineCS/comments/n4d0yx/cs_372_project_2/ (pulled inspo from here)
# Required Textbook readings

from segment import Segment

# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #


class RDTLayer(object):
    # ################################################################################################################ #
    # Class Scope Variables                                                                                            #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    DATA_LENGTH = 4 # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0                                # Use this for segment 'timeouts'

    # ################################################################################################################ #
    # __init__()                                                                                                       #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0

        # Sender vars
        self.TIMEOUT = 5
        self.base = 0
        self.next_seq = 0
        self.send_buffer = {}

        # Receiver vars
        self.expected_seq = 0
        self.recv_buffer = {}
        self.received_data_buffer = []

        # Counts for segment issues (rest of counts are handled in unreliable.py)
        self.countSegmentTimeouts = 0

        # Ack variables
        self.dup_ack_count = 0
        self.last_ack_seen = None
        self.fast_retrans_dup_threshold = 1
        self.last_ack_sent = None

    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setDataToSend(self, data):
        self.dataToSend = data

        self.base = 0
        self.next_seq = 0
        self.sendbuffer = {}

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def getDataReceived(self):
        # ############################################################################################################ #
        # Identify the data that has been received...

        result = ''.join(self.received_data_buffer)
        return result

        # print('getDataReceived(): Complete this...')
        # ############################################################################################################ #

    # ################################################################################################################ #
    # _createDataSeg()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # creates a payload seg to be sent over a comm channel                                                             #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def _createDataSeg(self, segnum, payload):
        seg = Segment()
        seg.setData(str(segnum), payload)
        return seg

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processData(self):
        self.currentIteration += 1
        self.processSend()
        self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processSend(self):
        for seg, send_data in list(self.send_buffer.items()):
            # First, resend segments that timed out
            last_sent = send_data["last_sent"]
            if self.currentIteration - last_sent >= self.TIMEOUT:
                segment = send_data["segment"]
                self.countSegmentTimeouts += 1
                #print(f"Timeout: retransmitting seg {seg} at iter {self.currentIteration} (timeouts = {self.countSegmentTimeouts})")
                send_data["last_sent"] = self.currentIteration
                self.sendChannel.send(segment)

        # Second, send new segments if sliding window has space
        end_of_window = self.base + RDTLayer.FLOW_CONTROL_WIN_SIZE
        while (self.next_seq < end_of_window) and (self.next_seq < len(self.dataToSend)):
            start = self.next_seq
            end = min(start + RDTLayer.DATA_LENGTH, len(self.dataToSend))
            chunk = self.dataToSend[start:end]
            seqnum = start  # sequence number in character index

            # Create segment
            seg = self._createDataSeg(seqnum, chunk)

            # Store in send_buffer including original payload
            self.send_buffer[seqnum] = {
                "segment": seg,
                "last_sent": self.currentIteration,
                "payload": chunk,    # store original payload for fast-retransmit
                "seqnum": seqnum
            }

            print("Sending segment:", seg.to_string())
            self.sendChannel.send(seg)

            self.next_seq = end

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):
        segmentAck = Segment()
        listIncomingSegments = self.receiveChannel.receive()

        if not listIncomingSegments:
            listIncomingSegments = []

        # Partition: verify checksum once and separate ACK and DATA lists
        acks_list = []
        datas_list = []
        fast_retrans_threshold = getattr(self, "fast_retrans_dup_threshold", 3)

        for seg in listIncomingSegments:
            # Verify checksum once and drop corrupted segments entirely
            try:
                if not seg.checkChecksum():
                    # Corrupted segment: drop it
                    continue
            except Exception:
                continue

            if getattr(seg, "acknum", -1) != -1:
                acks_list.append(seg)

            if getattr(seg, "seqnum", -1) != -1:
                datas_list.append(seg)

        # -----------------------------
        # ACK pass: only valid ACKs (checksum already verified)
        # -----------------------------
        for seg in acks_list:
            try:
                # ensure acknum is an int
                acknum = int(str(seg.acknum))
            except Exception:
                # error: skip ack handling for this segment
                continue

            # duplicate ack tracking (only for valid ACKs)
            if getattr(self, "last_ack_seen", None) is None or acknum != self.last_ack_seen:
                self.last_ack_seen = acknum
                self.dup_ack_count = 0
            else:
                self.dup_ack_count = getattr(self, "dup_ack_count", 0) + 1

            # If we reached the threshold, fast-retransmit the earliest outstanding segment
            if getattr(self, "dup_ack_count", 0) >= fast_retrans_threshold:
                if self.send_buffer:
                    earliest_seq = min(self.send_buffer.keys())
                    entry = self.send_buffer.get(earliest_seq)
                    if entry is not None:
                        # rebuild a fresh Segment from stored payload/seqnum
                        orig_payload = entry.get("payload", "")
                        orig_seq = entry.get("seqnum", earliest_seq)
                        try:
                            new_seg = Segment()
                            new_seg.setData(str(orig_seq), orig_payload)
                            entry["segment"] = new_seg
                            entry["last_sent"] = getattr(self, "currentIteration", 0)
                            self.sendChannel.send(new_seg)
                        except Exception:
                            pass

                # reset duplicate counter to prevent endless retrans
                self.dup_ack_count = 0

            # Remove any fully-acked segments based on this acknum
            removable = []
            # iterate over a copy since we may mutate send_buffer
            for seq, entry in list(self.send_buffer.items()):
                stored_seg = entry.get("segment")
                payload = getattr(stored_seg, "payload", "") if stored_seg is not None else entry.get("payload", "")
                try:
                    payload_len = len(str(payload)) if payload is not None else 0
                except Exception:
                    payload_len = 0

                last_covered = seq + payload_len - 1
                if payload_len == 0:
                    if seq <= acknum:
                        removable.append(seq)
                else:
                    if last_covered <= acknum:
                        removable.append(seq)

            # remove ACKs from buffer since seg was processed
            for seq in removable:
                try:
                    del self.send_buffer[seq]
                except KeyError:
                    pass

            # slide base pointer
            if self.send_buffer:
                try:
                    self.base = min(self.send_buffer.keys())
                except Exception:
                    self.base = getattr(self, "next_seq", 0)
            else:
                self.base = getattr(self, "next_seq", 0)

        # -----------------------------
        # DATA pass: process valid DATA segments (checksum already verified)
        # -----------------------------
        for seg in datas_list:
            try:
                seqnum = int(str(seg.seqnum))
            except Exception:
                # error: skip data processing
                continue

            data_chunk = "" if seg.payload is None else str(seg.payload)

            # already delivered: can skip
            if seqnum + len(data_chunk) <= getattr(self, "expected_seq", 0):
                continue

            # if starts at or after expected_seq, buffer it
            if seqnum >= getattr(self, "expected_seq", 0):
                if seqnum not in self.recv_buffer:
                    self.recv_buffer[seqnum] = data_chunk
                else:
                    pass
            else:
                # partial overlap: accept suffix that is new
                overlap = getattr(self, "expected_seq", 0) - seqnum
                if overlap < len(data_chunk):
                    suffix = data_chunk[overlap:]
                    new_seq = getattr(self, "expected_seq", 0)
                    if new_seq not in self.recv_buffer:
                        self.recv_buffer[new_seq] = suffix

            # deliver any contiguous buffered data starting at expected_seq
            progressed = True
            while progressed:
                progressed = False
                if getattr(self, "expected_seq", 0) in self.recv_buffer:
                    chunk = self.recv_buffer.pop(self.expected_seq)
                    # append to received_data_buffer
                    try:
                        self.received_data_buffer.append(chunk)
                    except Exception:
                        # if buffer missing, create it
                        if not hasattr(self, "received_data_buffer"):
                            self.received_data_buffer = [chunk]
                        else:
                            pass

                    # advance expected_seq by delivered length and mark progressed as true
                    self.expected_seq += len(chunk)
                    progressed = True

        # -----------------------------
        # Final step: send cumulative ACK for everything we have in-order
        # -----------------------------
        try:
            acknum_to_send = getattr(self, "expected_seq", 0) - 1  # last contiguous byte index received (inclusive)
            # create and send ACK
            ack_str = str(acknum_to_send)
            segmentAck.setAck(ack_str)
            print(f"Length of Receive Unacked Packets List: {len(self.recv_buffer)}")
            print("Sending ack: ", segmentAck.to_string())
            self.sendChannel.send(segmentAck)
        except Exception as e:
            print("Failed to create/send ACK:", e)
            pass
