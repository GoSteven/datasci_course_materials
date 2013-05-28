import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    docId = record[0]
    text = record[1]
    words = text.split()
    for w in words:
      mr.emit_intermediate(w, [docId])

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = sorted(reduce(set().union, list_of_values))
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
