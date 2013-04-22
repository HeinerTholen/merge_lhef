
import itertools
import glob
import sys

def gen_open(filenames):
    """opens files"""
    for name in filenames:
        if name.endswith(".gz"):
            yield gzip.open(name)
        elif name.endswith(".bz2"):
            yield bz2.BZ2File(name)
        else:
            yield open(name)

def gen_cat(sources):
    """yield lines from files"""
    for s in sources:
        for item in s:
            yield item

def gen_sized_chunks(source, num):
    """makes chunks with num items"""
    class keyfunc(object):
        def __init__(self, num):
            self.current = -1
            self.num = int(num)
        def __call__(self, arg):
            self.current += 1
            return self.current / self.num
    groupby = itertools.groupby(source, keyfunc(num))
    return (g for k,g in groupby)

def event_block(lines):
    """yields generators over single events"""
    def block_gen(firstline, stream):
        yield firstline
        for line in stream:
            yield line
            if "</event>" in line: 
                break
    for line in lines:
        if "<event>" in line:
            yield block_gen(line, lines)

def parse_header(lines):
    """returns list of header lines"""
    header = []
    for line in lines:
        header.append(line)
        if "</init>" in line:
            break
    return header

def usage():
    print "usage: merge_lhef <no per file> <input glob str> <output base name>"
    print "(input glob str with * should be '')"
    print "sys.argv:"
    print sys.argv

def main():
    """Reads lhef and packs them in new files."""
    if len(sys.argv) != 4:
        usage()
        exit(-1)

    lhef = glob.glob(sys.argv[2])
    lhef = gen_open(lhef)
    lhef = gen_cat(lhef)

    header = parse_header(lhef)
    events = event_block(lhef)

    events_per_file = int(sys.argv[1])
    file_blocks = gen_sized_chunks(events, events_per_file)

    file_no = -1
    out_base_name = sys.argv[3]
    for fb in file_blocks:
        file_no += 1
        with open(out_base_name+str(file_no)+".lhef", "w") as f:
            f.writelines(header)
            for line in fb:
                f.writelines(line)
            f.writelines(("</LesHouchesEvents>\n",))

if __name__ == "__main__":
    main()


