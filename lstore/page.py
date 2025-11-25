

import struct

PAGE_SIZE = 4096          # bytes
INT_SIZE = 8              # 64-bit signed integer
MAX_RECORDS = PAGE_SIZE // INT_SIZE   # = 512 integers per page


class Page:

    def __init__(self):
        # raw 4096 bytes
        self.data = bytearray(PAGE_SIZE)

        # number of integers written into this page
        self.num_records = 0

    # -----------------------------------------------------------
    #  CAPACITY CHECK
    # -----------------------------------------------------------
    def has_capacity(self):
        """
        True if the page still has space for another 64-bit integer.
        """
        return self.num_records < MAX_RECORDS

    # -----------------------------------------------------------
    #  WRITE INTEGER
    # -----------------------------------------------------------
    def write(self, value):
        """
        Writes one 64-bit integer into the page.
        Stored sequentially at:
            offset = num_records * 8 bytes
        """
        if not self.has_capacity():
            raise Exception("Page is full")

        offset = self.num_records * INT_SIZE
        struct.pack_into("q", self.data, offset, value)
        self.num_records += 1

    # -----------------------------------------------------------
    #  READ INTEGER  (optional helper)
    # -----------------------------------------------------------
    def read(self, index):
        """
        Read 64-bit integer at the given record index.
        """
        if index >= self.num_records:
            raise Exception("Index out of bounds")

        offset = index * INT_SIZE
        return struct.unpack_from("q", self.data, offset)[0]

    # -----------------------------------------------------------
    #  SERIALIZATION FOR DISK I/O
    # -----------------------------------------------------------
    def to_bytes(self):
        """
        Convert the entire page to raw bytes.
        Always returns a 4096-byte object.
        """
        return bytes(self.data)

    @classmethod
    def from_bytes(cls, byte_data):
        """
        Create a Page from raw 4096-byte data.
        IMPORTANT:
            We cannot perfectly reconstruct num_records here,
            because zero is a valid stored int. So Phase 1 assumes:
                num_records = MAX_RECORDS
            This is refined in Phase 2/3 with metadata if needed.
        """
        if len(byte_data) != PAGE_SIZE:
            raise Exception("Invalid page size for from_bytes")

        page = cls()
        page.data = bytearray(byte_data)
        page.num_records = MAX_RECORDS  # best safe assumption for A2

        return page
