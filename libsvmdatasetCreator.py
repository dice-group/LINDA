import collections

operators = list()
subject = list()
subjectOperatorMap = {}
operator2Id = collections.OrderedDict()
i = 1


def construct_line(label, input):
    new_line = [label]
    indexes = sorted(input, key=int)
    for item in indexes:
        new_item = "%s:%s" % (item, 1)
        new_line.append(new_item)
    new_line = " ".join(new_line)
    new_line += "\n"
    return new_line


with open("/Users/Kunal/workspaceThesis/LINDA/Data/rdf.nt") as f:
    for line in f:
        data = line.split(" ")
        operator = data[1] + "::" + data[2]
        if operator not in operator2Id:
            operator2Id[operator] = i
            i += 1
        subjectOperatorMap.setdefault(data[0], []).append(operator)

with open('/Users/Kunal/workspaceThesis/LINDA/Data/rdf/operator2Id.csv', 'w') as mapfile:
    for operator, id in operator2Id.iteritems():
        mapfile.write( operator +"," +str(id)+"\n")
    mapfile.close


for operator, id in operator2Id.iteritems():
   with open("/Users/Kunal/workspaceThesis/LINDA/Data/DT/rdf/LIBSVMDATA/" + str(id) +".libsvm", "a") as libfile:
       for subject, operatorList in subjectOperatorMap.iteritems():
           if operator in operatorList:
               libfile.write(construct_line("+1", [operator2Id[item] for item in operatorList if item !=operator] ))
           else:
               libfile.write(construct_line("-1", [operator2Id[item] for item in operatorList]))
       libfile.close
