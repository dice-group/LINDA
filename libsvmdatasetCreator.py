import collections
from multiprocessing import Pool
import multiprocessing

operators = list()
subject = list()
subjectOperatorMap = {}
operator2Id = collections.OrderedDict()
i = 1

def process_files(operator):
    with open("/Users/Kunal/workspaceThesis/LINDA/Data/freebase_mtr100_mte100-train/LIBSVM/" + str(operator2Id[operator]) +".libsvm", "a") as libfile:
        for subject, operatorList in subjectOperatorMap.iteritems():
            if operator in operatorList:
                libfile.write(construct_line("+1", [operator2Id[item] for item in operatorList if operator2Id[item] !=operator] ))
            else:
                libfile.write(construct_line("-1", [operator2Id[item] for item in operatorList]))
    libfile.close

def construct_line(label, input):
    new_line = [label]
    indexes = sorted(input, key=int)
    for item in indexes:
        new_item = "%s:%s" % (item, 1)
        new_line.append(new_item)
    new_line = " ".join(new_line)
    new_line += "\n"
    return new_line


with open("/Users/Kunal/workspaceThesis/data 2/benchmark/fb15k/freebase_mtr100_mte100-train.nt") as f:
    for line in f:
        if(" " not in line)
            continue
        data = line.split(" ")
        operator = data[1] + "::" + data[2]
        if operator not in operator2Id.iteritems():
            operator2Id[operator] = i
            i += 1
        print(operator2Id[operator])
        subjectOperatorMap.setdefault(data[0], []).append(operator)


with open('/Users/Kunal/workspaceThesis/LINDA/Data/freebase_mtr100_mte100-train/operator2Id.csv', 'w') as mapfile:
   for  operator, id in operator2Id.iteritems():
       mapfile.write( operator +"," +str(id)+"\n")
mapfile.close


num_cores = multiprocessing.cpu_count()
print "Cores: ", num_cores
p = Pool(num_cores)
p.map(process_files, operator2Id.keys())
