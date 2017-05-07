import gzip, bz2
import dpkt
from dpkt import bgp, mrt

BZ2_MAGIC = "\x42\x5a\x68"
GZIP_MAGIC = dpkt.gzip.GZIP_MAGIC
MRT_HEADER_LEN = dpkt.mrt.MRTHeader.__hdr_len__

class BGPTableDump:
    def __init__(self, filename):
        f = file(filename, "rb")
        hdr = f.read(max(len(BZ2_MAGIC), len(GZIP_MAGIC)))
        f.close()

        if filename.endswith(".bz2") and hdr.startswith(BZ2_MAGIC):
            self.fobj = bz2.BZ2File
        elif filename.endswith(".gz") and hdr.startswith(GZIP_MAGIC):
            self.fobj = gzip.GzipFile
        else:
            self.fobj = file
        self.open(filename)

    def open(self, filename):
        self.f = self.fobj(filename, "rb")

    def close(self):
        self.f.close()
        raise StopIteration

    def __iter__(self):
        return self

    def next(self):
        while True:
            s = self.f.read(MRT_HEADER_LEN)
            if len(s) < MRT_HEADER_LEN:
                self.close()

            mrt_h = mrt.MRTHeader(s)
            s = self.f.read(mrt_h.len)
            if len(s) < mrt_h.len:
                self.close()

            if mrt_h.type != mrt.TABLE_DUMP:
                continue

            if mrt_h.subtype == mrt.BGP4MP_MESSAGE:
                bgp_h = mrt.BGP4MPMessage(s)
            elif mrt_h.subtype == mrt.BGP4MP_MESSAGE_32BIT_AS:
                bgp_h = mrt.BGP4MPMessage_32(s)
            else:
                continue
            bgp_m = mrt.TableDump(s)
            break
        return bgp_m
