import math
from random import shuffle
import sys

vectors_filename = sys.argv[1]
if len(sys.argv) > 2:
    exclusion_filename = sys.argv[2]
    with open(exclusion_filename) as f:
        exclusion_rids = f.read().split("\n")
else:
    exclusion_rids = []

with open(vectors_filename) as f:
    rids = f.read().split("\n")


rids = [rid for rid in rids if rid not in exclusion_rids]
shuffle(rids)

rid_count = len(rids)
train_index = math.floor(rid_count * 0.1)
test_rids = rids[:train_index]
train_rids = rids[train_index:]

with open("train_rids.txt", "w") as f:
    f.write("\n".join(train_rids))

with open("test_rids.txt", "w") as f:
    f.write("\n".join(test_rids))
