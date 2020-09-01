import sys
from uniques import get_recent

filename = sys.argv[1]

uid = sys.argv[1]

rids = sys.argv[2:]

print(get_recent(rids))
