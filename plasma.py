import random

import pandas as pd
from pyplasma import ObjectID
import pyplasma
import pyarrow as pa


class PlasmaClient(object):
    def __init__(self, store_socket_name: str):
        super().__init__()
        self.client = pyplasma.connect(store_socket_name, -1)

    def disconnect(self):
        """
        Disconnect this client from the Plasma store.
        """
        self.client.disconnect()

    @staticmethod
    def get_object_id(object_id_str: bytes = None):
        """
        Returns a generated ObjectID.if object id is None,return random generated ObjectID
        Returns
        -------
        ObjectID: A randomly generated ObjectID.
        """
        if object_id_str is None:
            object_id_str = bytes(bytearray(
                random.getrandbits(8) for _ in range(pyplasma.ObjectID.size())))
        return pyplasma.ObjectID.from_binary(object_id_str)

    def subscribe(self):
        """Subscribe to notifications about sealed objects."""
        self.client.subscribe()

    def seal(self, object_id: pyplasma.ObjectID):
        self.client.seal(object_id)

    def create(self, object_id: ObjectID, data_size, meta_data=b""):
        """
        Create a new buffer in the PlasmaStore for a particular object ID.
       The returned buffer is mutable until ``seal()`` is called.
      Parameters
      ----------
      object_id : ObjectID
          The object ID used to identify an object.
      data_size : int
          The size in bytes of the created buffer.
      meta_data : bytes
          An optional string of bytes encoding whatever metadata the user
          wishes to encode.
      Returns
      -------
      buffer : Buffer
          A mutable buffer where to write the object data.
      Raises
      ------
      PlasmaObjectExists
          This exception is raised if the object could not be created because
          there already is an object with the same ID in the plasma store.

      PlasmaStoreFull
          This exception is raised if the object could
          not be created because the plasma store is unable to evict
          enough objects to create room for it.
      """
        buffer = self.client.create(object_id, data_size, meta_data)
        return buffer

    def write_df(self, object_id: ObjectID, value: pd.DataFrame, meta_data=""):
        """
        Store a Python value into the object store.
        Parameters
        ----------
        value : object
            A Python object to store.
        object_id : ObjectID, default None
            If this is provided, the specified object ID will be used to refer
            to the object.
        Returns
        -------
        The object ID associated to the Python object.
        """
        table = pa.Table.from_pandas(value)
        sink = pa.MockOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buffer_size = sink.size()
        # create a buffer and it's size is buffer_size
        buffer = self.client.create(object_id, buffer_size, meta_data)
        # using pyarrow ipc to write to buffer or other ways
        stream = pa.FixedSizeBufferWriter(buffer)
        with pa.RecordBatchStreamWriter(stream, table.schema) as writer:
            writer.write_table(table)
        # write finished,submit to plasma store server
        self.client.seal(object_id)

    def get_notification_socket(self):
        """
        Get the notification socket.
        """
        import socket
        return socket.socket(fileno=self.client.fd(), family=socket.AF_UNIX, type=socket.SOCK_STREAM)

    def debug_string(self):
        return self.client.debug_string()

    def contains(self, object_id: pyplasma.ObjectID) -> bool:
        """
        Check if the object is present and sealed in the PlasmaStore.
        Parameters
        ----------
        object_id : ObjectID
            A string used to identify an object.
        """
        return self.client.contains(object_id)

    def evict(self, num_bytes: int) -> int:
        """
        Evict some objects until to recover some bytes.
        Recover at least num_bytes bytes if possible.
        Parameters
        ----------
        num_bytes : int
        The number of bytes to attempt to recover.
        """
        return self.client.evict(num_bytes)

    def get_next_notification(self):
        """
        Get the next notification from the notification socket.
        Returns
        -------
        ObjectID
            The object ID of the object that was stored.
        int
            The data size of the object that was stored.
        int
            The metadata size of the object that was stored.
        """
        return self.client.get_next_notification()

    def remove(self, object_ids: list[pyplasma.ObjectID]):
        """
        Delete the objects with the given IDs from other object store.
        Parameters
        ----------
        object_ids : list
            A list of strings used to identify the objects.
        """
        self.client.remove(object_ids)

    def set_client_options(self, client_name: str, limit_output_memory: int):
        self.client.set_client_options(client_name, limit_output_memory)

    def store_capacity(self) -> int:
        """
        Get the memory capacity of the store.
        Returns
        -------
        int
          The memory capacity of the store in bytes.
        """
        return self.client.store_capacity()

    def read_df(self, object_id: pyplasma.ObjectID, timeout_ms: int = -1):
        """
        Get one or more Python values from the object store.
            Parameters
            ----------
            object_id : ObjectID
                Object ID associated to the values we get
                from the store.
            timeout_ms : int, default -1
                The number of milliseconds that the get call should block before
                    timing out and returning. Pass -1 if the call should block and 0
                    if the call should return immediately.
            Returns
                -------
                object
                    pandas.DataFrame
            """
        buffer = self.client.get_buffer(object_id, timeout_ms)
        reader = pa.BufferReader(buffer)
        table = pa.ipc.open_stream(reader).read_all()
        return table.to_pandas()

    def get_buffer(self, object_id: pyplasma.ObjectID, timeout_ms: int = -1):
        """
            Get one or more Python values from the object store.
            Parameters
            ----------
            object_id : ObjectID
                Object ID associated to the values we get
                from the store.
            timeout_ms : int, default -1
                The number of milliseconds that the get call should block before
                timing out and returning. Pass -1 if the call should block and 0
                if the call should return immediately.
            Returns
            -------
            object
                Python value or list of Python values for the data associated with
                the object_ids and ObjectNotAvailable if the object was not available.
        """
        buffer = self.client.get_buffer(object_id, timeout_ms)
        return buffer

    def get_meta_buffer(self, object_id: pyplasma.ObjectID, timeout_ms: int = -1):
        """
        Returns metadata buffer from the PlasmaStore based on object ID.
        If the object has not been sealed yet, this call will block. The
        retrieved buffer is immutable.
        Parameters
        ----------
        object_ids : list
            A list of ObjectIDs used to identify some objects.
        timeout_ms : int
            The number of milliseconds that the get call should block before
            timing out and returning. Pass -1 if the call should block and 0
            if the call should return immediately.
        Returns
        -------
        list
            List of PlasmaBuffers for the metadata associated with the
            object_ids and None if the object was not available.
        """
        buffer = self.client.get_meta_buffer(object_id, timeout_ms)
        return buffer

    def get_display_info(self):
        """
        Experimental: List the objects in the store.
        Returns
        -------
        dict
            Dictionary from ObjectIDs to an "info" dictionary describing the
            object. The "info" dictionary has the following entries:
            data_size
                size of the object in bytes
            metadata_size
                size of the object metadata in bytes
            ref_count
                Number of clients referencing the object buffer
            create_time
                Unix timestamp of the creation of the object
            construct_duration
                Time the creation of the object took in seconds
            state
            "created" if the object is still being created and
            "sealed" if it is already sealed
       """
        return self.client.list()
