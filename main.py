#!/usr/bin/env python

from optparse import OptionParser
from dpkt import bgp
from BGPTableDump import BGPTableDump
import time
import datetime
import os
import sys
import networkx as nx

DATA_FOLDER = "BGPDumpData/"
FILE_LIST_FNAME = "fileList.txt"
DATA_INFO = "dataInfo.txt"
ALL_AS_PATH_FNAME = "allPath.txt"
GRAPH_DATA_FNAME = "graphData.txt"
AS_REL_FOLDER = "ASRelationshipData/"

def aspath_to_str(as_path):
    str = ""
    for seg in as_path.segments:
        if seg.type == bgp.AS_SET:
            continue
        elif seg.type == bgp.AS_SEQUENCE:
            start = ""
            end = " "
        else:
            start = "?%d?" % (seg.type)
            end = "? "
        str += start
        prevAS = -1
        for AS in seg.path:
            if prevAS != AS:
                str += "%d " % (AS)
                prevAS = AS
        str = str[:-1]
        str += end
    str = str[:-1]
    return str

def CheckNewData(fileList):
    #read fileList.txt
    with open(FILE_LIST_FNAME) as f:
        content = f.readlines()
    content = [x.strip() for x in content]

    if (len(content) != len(fileList)):
        return True
    for record in content:
        if record not in fileList:
            return True
    return False

def CountBGPEntries():
    count = 0
    fileList = os.listdir(DATA_FOLDER)
    print "Start Counting..."
    for file in fileList:
        print "File: ", file
        parser = OptionParser()
        parser.add_option("-i", "--input", dest="input", default=DATA_FOLDER + file,
                          help="read input from FILE", metavar="FILE")
        (options, args) = parser.parse_args()

        dump = BGPTableDump(options.input)

        for bgp_m in dump:
            count += 1
    print "Counting Done: %s record" % count
    return count

def DumpingASPathToText(fileList):
    #read from dataInfo.txt
    with open(DATA_INFO) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    for line in content:
        data = line.split(' ')
        if (data[0] == 'TotalRecord'):
            totalEntries = int(data[1])
    print "Starting processing dump...", datetime.datetime.now().time()
    count = 0
    prevProgress = 0
    currProgress = 0
    f = open("allPath.txt", "w")
    f.truncate()
    for file in fileList:
        parser = OptionParser()
        parser.add_option("-i", "--input", dest="input", default=DATA_FOLDER + file,
                          help="read input from FILE", metavar="FILE")
        (options, args) = parser.parse_args()

        dump = BGPTableDump(options.input)

        for bgp_m in dump:
            for attr in bgp_m.attributes:
                if attr.type == bgp.AS_PATH:
                    f.write("%s\n" % aspath_to_str(attr.as_path))
                    count += 1
                    currProgress = float(count) / totalEntries * 100
                    if (int(currProgress*100) != int(prevProgress*100)):
                        prevProgress = currProgress
                        sys.stdout.write("\r%2.2f%%" % currProgress)
                        sys.stdout.flush()
    f.close()
    print "\nFinish", datetime.datetime.now().time()

def BuildGraphFromBGPData():
    print "\nStart Constructing Graph"
    G = nx.DiGraph()
    #read AS Paths and build graph
    with open(ALL_AS_PATH_FNAME) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    fullList = []
    maxAS = 0
    for line in content:
        asList = map(int, line.split(" "))
        pathLen = len(asList)
        for i in range(0, pathLen - 1):
            G.add_edge(asList[i], asList[i + 1], relationship=0)
            G.add_edge(asList[i + 1], asList[i], relationship=0)

    nx.write_edgelist(G, GRAPH_DATA_FNAME)
    print "Nodes: ", G.number_of_nodes()
    print "Edges: ", G.number_of_edges()
    return G

def UpdateEdgeRelationship(G):
    print "Updating Graph relation: Start"
    fileList = os.listdir(AS_REL_FOLDER)
    for fileName in fileList:
        with open(AS_REL_FOLDER + fileName) as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        for line in content:
            if (line[0] == "#"):
                continue
            #data format: <AS1><AS2><relationship>
            #<provider-as>|<customer-as>|-1
            #<peer-as>|<peer-as>|0
            data = line.split("|")
            if G.has_edge(data[0], data[1]):
                G.get_edge_data(data[0], data[1])["relationship"] = data[2]
                G.get_edge_data(data[1], data[0])["relationship"] = str(int(data[2]) * -1)
    nx.write_edgelist(G, GRAPH_DATA_FNAME)
    print "Updating Graph relation: Done"
###############################################################################
# Main
###############################################################################
def main():
    fileList = os.listdir(DATA_FOLDER)
    #check if the file list is updated
    hasNewData = CheckNewData(fileList)
    ####################################
    # New Data
    ####################################
    if (hasNewData):
        #count entries again
        totalEntries = CountBGPEntries()
        #
        f = open(FILE_LIST_FNAME, "w")
        for item in fileList:
            f.write("%s\n" % item)
        f.close()
        DumpingASPathToText(fileList)
        G = BuildGraphFromBGPData()
        UpdateEdgeRelationship(G)
    ####################################
    # No new Data
    ####################################
    else:
        print "Data has no change"
        print "Reading Graph Data"
        G = nx.read_edgelist(GRAPH_DATA_FNAME, create_using=nx.DiGraph(), data=True)
        print G.number_of_nodes(), G.number_of_edges()

        Gcopy = G.copy()
        #delete downhill and peer path
        for edge in G.edges(data=True):
            if edge[2]["relationship"] != "1":
                #remove edge
                Gcopy.remove_edge(edge[0], edge[1])
        #calculating shortest uphill path
        allShortestUphillPath = nx.all_pairs_shortest_path(Gcopy)
        #read allPath.txt for testset data
        count = 0
        matchCount = 0
        matchLengthCount = 0
        exactMatchCount = 0
        shorterCount = 0
        longerCount = 0
        with open("allPath.txt") as f:
            uniquePaths = set()
            for line in f:
                data = line.strip().split(" ")
                if (len(data) <= 2):
                    continue
                ####################################
                # MEASUREMENT: By ASN
                # check accuracy for AS XXXX
                ####################################
                ASN = u"8121"
                if (data[0] != ASN):
                    continue
                if line in uniquePaths:
                    continue
                else:
                    uniquePaths.add(line)
                ####################################
                # End Measurement by ASN
                ####################################
                finalCost, finalPaths = InferASAPath(data[0], data[-1], G, allShortestUphillPath, None)
                #count test
                count += 1
                #check test result
                if data in finalPaths:
                    matchCount += 1
                    if (len(finalPaths) == 1):
                        exactMatchCount += 1
                #check length
                if (finalCost == len(data)):
                    matchLengthCount += 1
                elif (finalCost > len(data)):
                    longerCount += 1
                else:
                    shorterCount += 1
                #test for 100
                if count % 100 == 0:
                    print "Test %d - match %d - exact %d, %f percent" % (count, matchCount, exactMatchCount, float(matchCount)/count*100)
                #if count == 100000:
                #    break
            print "-----------------------------------------------------------"
            print "AS", ASN
            print "Test %d" % (count)
            print "Match %d, %f percent" % (matchCount, float(matchCount)/count*100)
            print "Exact Match %d, %f percent" % (exactMatchCount, float(exactMatchCount)/count*100)
            print "Match Length %f, Shorter %f, Longer %f" % (float(matchLengthCount)/count*100, float(shorterCount)/count*100, float(longerCount)/count*100)

###############################################################################
# Function to Infer AS Path
# with option to specify first hop to increase accuracy
# If no first hop is given, set firstHop = None
###############################################################################
def InferASAPath(src, dest, G, allShortestUphillPath, firstHop):
    #################################
    # cost without flat link
    # = uphill + downhill
    #################################
    cost0 = sys.maxint
    paths0 = []
    for m in G.nodes():
        if (allShortestUphillPath[src].has_key(m) and allShortestUphillPath[dest].has_key(m)):
            firstHalf = allShortestUphillPath[src][m]
            secondHalf = allShortestUphillPath[dest][m]
            path = firstHalf[0:-1] + secondHalf[::-1]
            temp = len(path)
            #check first hop
            if (firstHop != None):
                if (path[1] != firstHop):
                    continue
            if (temp < cost0):
                cost0 = temp
                #reset path result
                paths0 = []
                paths0.append(path)
            elif (temp == cost0):
                #another valid path
                paths0.append(path)

    #################################
    # cost with flat link
    # = uphill + peer-peer + downhill
    # uphill OR downhill might = 0
    # but not both
    #################################
    cost1 = sys.maxint
    paths1 = []
    for m in G.nodes():
        #no uphill path from dest to m
        if not (allShortestUphillPath[dest].has_key(m)):
            continue
        #peers of m
        peers = []
        neighbors = G.neighbors(m)
        for n in neighbors:
            if G.get_edge_data(m, n)["relationship"] == "0":
                #add to peers
                peers.append(n)
        for p in peers:
            #no uphill path from src to p
            if not (allShortestUphillPath[src].has_key(p)):
                continue
            firstHalf = allShortestUphillPath[src][p]
            secondHalf = allShortestUphillPath[dest][m]
            temp = len(firstHalf) + len(secondHalf)
            path = firstHalf + secondHalf[::-1]
            #check first hop
            if (firstHop != None):
                if (path[1] != firstHop):
                    continue
            if (temp < cost1):
                cost1 = temp
                paths1 = []
                paths1.append(path)
            elif (temp == cost1):
                paths1.append(path)
    ##################################
    #Combining all paths to result
    ##################################
    finalCost = min(cost0, cost1)
    finalPaths = []
    if cost0 == finalCost:
        finalPaths += paths0
    if cost1 == finalCost:
        finalPaths += paths1
    return finalCost, finalPaths

if __name__ == "__main__":
    main()
