from mpi4py import MPI
import re, json, sys, math, time
import twitter.mpi_logger as log
from collections import Counter, defaultdict


# Constants
MASTER_RANK = 0
MAX_BUFF = int((2**31 - 1) / 2)

# Language File
LANG_FILE = "data/languages.json"

# Regex to find individual doc.
DOC_REGEX = b'doc":.*?"text".*?".*?"(?:,).*"lang".*?,'

# Regex for Tweet text.
TEXT_REGEX = b'(?:"text":")(.*?)(?:")'

# Regex for Tweet hashtag.
TAG_REGEX = b'#([a-zA-Z0-9_]+)'

# Regex for Tweet language.
LANG_REGEX = b'(?:"lang":")(.*?)(?:")'

# setup logger
master_logger = log.MPILogger("master_logger")


def handle_file(comm, input_file, rank):
    try:
        # File pointer
        tweet_file = MPI.File.Open(comm, input_file, MPI.MODE_RDONLY)

        # File size
        tweet_file_size = MPI.File.Get_size(tweet_file)

        return tweet_file, tweet_file_size

    except:

        master_logger.error("Unable to open file at worker {}".format(rank))

        # Unable to process file.
        return None, 0


# Currently all processes work including master.
def process_tweets(comm, input_file, rank, processes):

    # Counters
    hashtags = defaultdict(int)
    languages = defaultdict(int)

    tweet_file, tweet_file_size = handle_file(
        comm=comm, input_file=input_file, rank=rank)

    # Unable to read file.
    if not tweet_file:
        return hashtags, languages

    # Determine chunk of file to read.
    if (processes - 1 != rank or processes == 1):

        # Number of file bytes to read in.
        file_chunk = int(math.ceil(tweet_file_size/processes))

        # Byte index to begin read from.
        file_offset = file_chunk * rank

    else:

        # Last portion of file to read in.
        file_chunk = (tweet_file_size %
                      int(math.ceil(tweet_file_size/processes)))

        file_offset = tweet_file_size - file_chunk - 1

    # Number of iterations for buffer.
    number_buffs = int(math.ceil(file_chunk / MAX_BUFF))

    for i in range(number_buffs):
        try:

            buff_size = int(math.ceil(file_chunk / number_buffs))

            buff = bytearray(buff_size)

            offset = file_offset + (i * buff_size)

            # Fill buffer starting at offset.
            tweet_file.Read_at_all(offset, buff)

            # Find all docs in buffer.
            docs = re.findall(DOC_REGEX, buff, re.I)

            for doc in docs:

                # Find first text attribute (more than 1 text field in doc).
                text = re.search(TEXT_REGEX, doc)

                # Get hashtags.
                if text:
                    tags = re.findall(TAG_REGEX, text.group(1))
                    for t in tags:
                        hashtags[t.lower()] += 1

                # Find language
                lang = re.search(LANG_REGEX, doc)
                if lang:
                    languages[lang.group(1)] += 1

        except:
            # Unable to open file portion.
            master_logger.error(
                "Unable to open file portion {}/{} at worker {}".format(i, number_buffs, rank))

    # Close file.
    tweet_file.Close()

    return hashtags, languages


def marshall_tweets(comm):

    size = comm.Get_size()
    results = []

    # Send request to slaves to return results.
    for i in range(size-1):

        comm.send('return_data', dest=(i+1), tag=(i+1))
        master_logger.log(
            "Requesting return data from worker {}".format(i + 1))

    # Collect results from slaves.
    for i in range(size-1):

        results.append(comm.recv(source=(i+1), tag=MASTER_RANK))
        master_logger.log("Received return data from worker {}".format(i + 1))

    return results


def slave_tweet_processor(comm, input_file):
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Process tweets.
    results = process_tweets(comm, input_file, rank, size)

    # Wait for master to request results.
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        if isinstance(in_comm, str):
            if in_comm in ("return_data"):

                # Send results to master.
                comm.send(results, dest=MASTER_RANK, tag=MASTER_RANK)

            elif in_comm in ("exit"):

                # Stop process.
                exit(0)


def master_tweet_processor(comm, input_file):

    rank = comm.Get_rank()
    size = comm.Get_size()

    hashtags, languages = process_tweets(comm, input_file, rank, size)

    if size > 1:

        master_logger.log("Processes > 1, size  {}".format(size))

        # Request data from slaves.
        results = marshall_tweets(comm)

        for tag_count, lang_count in results:

            # Hashtags.
            for k, v in tag_count.items():
                hashtags[k] += v

            # Languages.
            for k, v in lang_count.items():
                languages[k] += v

        # Send signal to stop slaves.
        exit_workers(comm=comm, size=size)

    log_results(hashtags=hashtags, languages=languages)


def exit_workers(comm, size):
    for i in range(size-1):

        # Send exit signal.
        comm.send('exit', dest=(i+1), tag=(i+1))
        master_logger.log("Sending exit signal to worker {}".format(i + 1))


def log_results(hashtags, languages):

    # Read in languages.
    with open(LANG_FILE, 'r') as file:
        lang_list = json.loads(file.read())

    # Print results.
    for (i, (j, k)) in enumerate(Counter(hashtags).most_common()[:10]):
        master_logger.log("{}. #{}, {}".format(i+1, j.decode('utf-8'), k))

    for i, (j, k) in enumerate(Counter(languages).most_common()[:10]):
        curr_lang = j.decode('utf-8').lower()
        master_logger.log("{}. {} ({}), {}".format(i+1,
                                                   lang_list[curr_lang], curr_lang, k))


def run(argv):

    # Get Twitter filename.
    input_file = argv[0]

    # Initialise comm.
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        # Master process.
        start = time.time()
        master_logger.log("Start script execution @ {}".format(time.asctime))
        master_tweet_processor(comm, input_file)
        master_logger.log("Finish script execution @ {}, total execution time: {}".format(
            time.asctime(), time.time() - start))
    else:
        # We are slave
        slave_tweet_processor(comm, input_file)
