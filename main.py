import networkx as nx
import pandas as pd
import re
import logging
from queue import Queue
from collections import defaultdict
from itertools import combinations

class IMDBGraph:
    def __init__(self):
        self.imdbFilePath = "imdb-actors-actresses-movies.tsv"
        self.provaFilePath = "prova.tsv"
        self.G = nx.Graph()
        self.A = nx.Graph()
        self.edgesDict = {}
        self.actorIdDict = {}
        self.movieIdDict = {}
        self.idActorDict = {}
        self.idMovieDict = {}
        self.movieYearDict = {}

        #logger configuration
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(filename="log.txt", filemode='w', format='%(asctime)s %(message)s', level=logging.INFO) 
        self.logger=logging.getLogger()
        self.logger.info('bult object IMDBGraph')

    def extractData(self):
        data = pd.read_csv(self.imdbFilePath, sep="\t")
        #data = pd.read_csv(self.provaFilePath, sep="\t")
        self.edgesDict = data.to_dict("index")
        self.idActorDict = dict(enumerate(data["Actor"].unique()))
        self.actorIdDict = {v: k for k ,v in self.idActorDict.items()}
        self.idMovieDict = dict(enumerate(data["Movie"].unique(), start=len(self.idActorDict)))
        self.movieIdDict = {v: k for k, v in self.idMovieDict.items()}
        self.movieYearDict= dict(map(self.getYear,data["Movie"].unique()))
        self.logger.info('extracted data from file and created dictionaries')

    def buildGraph(self):
        self.G.add_nodes_from([(id, {"name": a, "type": 0}) for id, a in self.idActorDict.items()])
        self.G.add_nodes_from([(id, {"name": m, "type": 1, "year": self.movieYearDict[m]}) for id, m in self.idMovieDict.items()])
        self.G.add_edges_from([(self.actorIdDict[v["Actor"]], self.movieIdDict[v["Movie"]]) for k, v in self.edgesDict.items()])
        self.logger.info('built the main graph')

    def getYear(self, movieString):
        match = re.search(".*\(.*(\d\d\d\d).*\)", movieString)
        if match is None:
            year = "Not found"
        else:
            yearString = match.group(1)
            year = int(yearString)
        return movieString, year


    def customBFS(self, LCC, startNode):
        visited = {}
        queue = Queue()
        queue.put(startNode)
        #node startNode is at level(distance) equale to 0
        visited[startNode] = 0
        while not queue.empty():
            currentNode = queue.get()
            for nextNode in LCC.neighbors(currentNode):
                if nextNode not in visited:
                    queue.put(nextNode)
                    visited[nextNode]=visited[currentNode]+1
        #Gruob by distance to create dict of distance
        Bu = defaultdict(list)
        for key, value in visited.items():
            Bu[value].append(key)
        return Bu

    def computeDiameter(self, LCC, Bu):
        i = lb = max(Bu)
        ub = 2*lb
        while ub > lb:
            self.logger.info ("entro nel while di computeDiameter")
            eccDict = nx.eccentricity(LCC, Bu[i])
            Bi = max(eccDict.values())
            maxVal = max(Bi,lb)
            if maxVal > 2*(i - 1):
                return maxVal
            else: 
                lb = maxVal
                ub = 2*(i - 1) 
            i = i - 1 
        return lb

    def computeAllDiameters(self):
        for maxYear in range (1930, 2030, 10):
            SG = self.G.subgraph([n for n, a in self.G.nodes().data() if a["type"] == 0 or (a["year"] != "Not found" and a["year"] < maxYear)])
            #tempLCC = max(list(nx.connected_components(SG)), key=len)
            tempLCC = sorted(nx.connected_components(SG), key=len, reverse=True)[0]
            LCC = SG.subgraph(tempLCC)
            degreeDict = dict(LCC.degree())
            m = max(degreeDict.values())
            startNode = [k for k, v in degreeDict.items() if v == m][0]
            #startNode = max(LCC.degree,key=lambda x: x[1])
            Bu = self.customBFS(LCC, startNode)
            d = self.computeDiameter(LCC, Bu)
            self.logger.info (f"the diameter of the graph relative to year {maxYear} is {d}")

    def maxCollaborations(self):
        collaborations = {}
        for a in self.G.nodes().data():
            if a[1]["type"] == 0:
                c = 0
                for n in self.G.neighbors(a[0]):
                    c += (len(self.G[n])-1)
                collaborations.update({a[1]["name"]: c})
        maxCollaborations = max(list(collaborations.values()))
        maxCollabActor = [k for k, v in collaborations.items() if v == maxCollaborations][0]
        self.logger.info (f"the actor with most collaborations ({maxCollaborations}) is {maxCollabActor}")
        return (maxCollabActor, maxCollaborations)










def main():
    obj = IMDBGraph()
    obj.extractData()
    obj.buildGraph()
    obj.maxCollaborations()
    obj.computeAllDiameters()
    


if __name__=="__main__":
    main()

