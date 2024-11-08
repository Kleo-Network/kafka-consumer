from tasks.db import *
from tasks.classify import * 
from tasks.upload import *
from tasks.pii import *

import sys
import time

from amazon_kclpy import kcl
from amazon_kclpy.v3 import processor


class RecordProcessor(processor.RecordProcessorBase):
    """
    A RecordProcessor processes data from a shard in a stream. Its methods will be called with this pattern:
    * initialize will be called once
    * process_records will be called zero or more times
    * shutdown will be called if this MultiLangDaemon instance loses the lease to this shard, or the shard ends due
        a scaling change.
    """

    def __init__(self):
        self._SLEEP_SECONDS = 5
        self._CHECKPOINT_RETRIES = 5
        self._CHECKPOINT_FREQ_SECONDS = 60
        self._largest_seq = (None, None)
        self._largest_sub_seq = None
        self._last_checkpoint_time = None

    @staticmethod
    def log(message):
        sys.stderr.write(message)

    def initialize(self, initialize_input):
        """
        Called once by a KCLProcess before any calls to process_records
        :param amazon_kclpy.messages.InitializeInput initialize_input: Information about the lease that this record
            processor has been assigned.
        """
        self._largest_seq = (None, None)
        self._last_checkpoint_time = time.time()

    def checkpoint(
        self, checkpointer, sequence_number=None, sub_sequence_number=None
    ):
        """
        Checkpoints with retries on retryable exceptions.
        :param amazon_kclpy.kcl.Checkpointer checkpointer: the checkpointer provided to either process_records
            or shutdown
        :param str or None sequence_number: the sequence number to checkpoint at.
        :param int or None sub_sequence_number: the sub sequence number to checkpoint at.
        """
        for n in range(0, self._CHECKPOINT_RETRIES):
            try:
                checkpointer.checkpoint(sequence_number, sub_sequence_number)
                return
            except kcl.CheckpointError as e:
                if "ShutdownException" == e.value:
                    #
                    # A ShutdownException indicates that this record processor should be shutdown. This is due to
                    # some failover event, e.g. another MultiLangDaemon has taken the lease for this shard.
                    #
                    print(
                        "Encountered shutdown exception, skipping checkpoint"
                    )
                    return
                elif "ThrottlingException" == e.value:
                    #
                    # A ThrottlingException indicates that one of our dependencies is is over burdened, e.g. too many
                    # dynamo writes. We will sleep temporarily to let it recover.
                    #
                    if self._CHECKPOINT_RETRIES - 1 == n:
                        sys.stderr.write(
                            "Failed to checkpoint after {n} attempts, giving up.\n".format(
                                n=n
                            )
                        )
                        return
                    else:
                        print(
                            "Was throttled while checkpointing, will attempt again in {s} seconds".format(
                                s=self._SLEEP_SECONDS
                            )
                        )
                elif "InvalidStateException" == e.value:
                    sys.stderr.write(
                        "MultiLangDaemon reported an invalid state while checkpointing.\n"
                    )
                else:  # Some other error
                    sys.stderr.write(
                        "Encountered an error while checkpointing, error was {e}.\n".format(
                            e=e
                        )
                    )
            time.sleep(self._SLEEP_SECONDS)

    def process_record(
        self, data, partition_key, sequence_number, sub_sequence_number
    ):
        """
        Called for each record that is passed to process_records.
        :param str data: The blob of data that was contained in the record.
        :param str partition_key: The key associated with this record.
        :param int sequence_number: The sequence number associated with this record.
        :param int sub_sequence_number: the sub sequence number associated with this record.
        """
        ####################################
        # Insert your processing logic here
        ####################################

        # call size calculation function and save to database from here itself 
        try:
            size = len(data)
            data_json = json.loads(data.decode("utf-8"))
            history_item = get_history_by_id(data_json["_id"])
            address = history_item["address"]
            user = increment_data_quantity(address, size)
        
        # update the database -> history update and activity_graph update 
            activity_json = user["activity_json"]
            activity = get_most_relevant_activity(data_json.get("summary", data_json.get("title", "")))
            if activity not in activity_json:
                activity_json[activity] = 1
            else:
                activity_json[activity] += 1
        
        
            if get_total_history_and_check_fifty(address):
                history_items = get_user_history(address)
                prepared_json = prepare_history_json(history_items, address, user)
                irys_hash = upload_to_arweave(prepared_json)
                update_user_by_address(address, {"activity_json": activity_json, "previous_hash": irys_hash})
            else:
                update_user_by_address(address, {"activity_json": activity_json})
            
            self.log("Activity {activity}, Data Size: {ds}".format(
                    activity=activity,
                    ds=size))
        except:
            pass

        

    def should_update_sequence(self, sequence_number, sub_sequence_number):
        """
        Determines whether a new larger sequence number is available
        :param int sequence_number: the sequence number from the current record
        :param int sub_sequence_number: the sub sequence number from the current record
        :return boolean: true if the largest sequence should be updated, false otherwise
        """
        return (
            self._largest_seq == (None, None)
            or sequence_number > self._largest_seq[0]
            or (
                sequence_number == self._largest_seq[0]
                and sub_sequence_number > self._largest_seq[1]
            )
        )

    def process_records(self, process_records_input):
        """
        Called by a KCLProcess with a list of records to be processed and a checkpointer which accepts sequence numbers
        from the records to indicate where in the stream to checkpoint.
        :param amazon_kclpy.messages.ProcessRecordsInput process_records_input: the records, and metadata about the
            records.
        """
        try:
            for record in process_records_input.records:
                data = record.binary_data
                seq = int(record.sequence_number)
                sub_seq = record.sub_sequence_number
                key = record.partition_key
                self.process_record(data, key, seq, sub_seq)
                if self.should_update_sequence(seq, sub_seq):
                    self._largest_seq = (seq, sub_seq)

            #
            # Checkpoints every self._CHECKPOINT_FREQ_SECONDS seconds
            #
            if (
                time.time() - self._last_checkpoint_time
                > self._CHECKPOINT_FREQ_SECONDS
            ):
                self.checkpoint(
                    process_records_input.checkpointer,
                    str(self._largest_seq[0]),
                    self._largest_seq[1],
                )
                self._last_checkpoint_time = time.time()

        except Exception as e:
            self.log(
                "Encountered an exception while processing records. Exception was {e}\n".format(
                    e=e
                )
            )

    def lease_lost(self, lease_lost_input):
        self.log("Lease has been lost")

    def shard_ended(self, shard_ended_input):
        self.log("Shard has ended checkpointing")
        shard_ended_input.checkpointer.checkpoint()

    def shutdown_requested(self, shutdown_requested_input):
        self.log("Shutdown has been requested, checkpointing.")
        shutdown_requested_input.checkpointer.checkpoint()


if __name__ == "__main__":
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()