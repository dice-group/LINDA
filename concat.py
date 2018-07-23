import subprocess

dir_in = "/kunal/DTAlgo/LIBSVMData"
dir_out = "/kunal/DTAlgo/FinalData"
args = "hdfs dfs -ls "+dir_in+" | awk '{print $8}'"

proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

def find_second_last(text, pattern):
   return text.rfind(pattern, 0, text.rfind(pattern))
subprocess.call("hdfs dfs -mkdir "+ dir_out, shell=True)
for line in proc.stdout.readlines():
    proc = subprocess.Popen("hdfs dfs -ls "+line, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()
    for line2 in all_dart_dirs:
        if "part" in line2:
            newName= line2[find_second_last(line2, '/'):line2.rfind('/')]+".libsvm"
            subprocess.call("hdfs dfs -mv " + line2 + " " +line2[:line2.rfind('/')] + newName, shell=True)
            subprocess.call("hdfs dfs -mv " + line2[:line2.rfind('/')] + newName + " " + dir_out, shell=True)
            # subprocess.call("hdfs dfs -mv " + line2 + " " + dir_in + line2[find_second_last(line2, '/'):line2.rfind('/')], shell=True)
