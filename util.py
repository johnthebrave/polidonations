
from pprint import pprint as pp
import re
import codecs, json

def load_dataset(path):

    print("Load dataset {}!".format(path))
    with open(path, encoding='utf-8') as f:
        data = json.loads(f.read())
    print("Loaded {} records!".format(len(data)))
    return data


def dump_dataset(path, data):

    print("Dump dataset {}: {}!".format(path, len(data)))
    with open(path, "w") as f:
        f.write(json.dumps(data, indent=4))


def print_defaultdict(data, max_items=None, verbose=True):
    data = sorted(data.items(), key=
    lambda kv: (kv[1], kv[0]), reverse=True)

    if verbose:
        if max_items:
            pp(data[:max_items])
        else:
            pp(data)

    return data
