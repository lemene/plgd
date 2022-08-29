import sys

import gzip
from Bio import SeqIO

def open_seq_file(fname, mode="r"):
    if fname[-6:] == ".fasta":
        return open(fname, mode), "fasta"
    elif fname[-6:] == ".fastq":
        return open(fname, mode), "fastq"
    elif fname[-9:] == ".fasta.gz":
        return gzip.open(fname, mode+"t"), "fasta"
    elif fname[-9:] == ".fastq.gz":
        return gzip.open(fname, mode+"t"), "fastq"
    else:
        return None, ""

def split_seqs(ifname, opattern, block_size):
    ifile, itype = open_seq_file(ifname)

    build_ofname = lambda i: opattern.replace("{}", str(i))

    index, base_size = 0, 0
    ofile, otype = open_seq_file(build_ofname(index), "w")
    for rec in SeqIO.parse(ifile, itype):
        SeqIO.write(rec, ofile, otype)
        base_size += len(rec.seq)
        if base_size >= block_size:
            index += 1
            base_size = 0
            ofile, otype = open_seq_file(build_ofname(index), "w")

        

split_seqs(sys.argv[1], sys.argv[2], int(sys.argv[3]))

