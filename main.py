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
        self.G.add_edges_from([(self.actorIdDict[v["Actor"]], self.movieIdDict[v["Movie"]]) for v in self.edgesDict.values()])
        self.logger.info('built the main graph')

    def getYear(self, movieString):
        match = re.search(".*\(.*(\d\d\d\d).*\)", movieString)
        if match is None:
            year = "Not found"
        else:
            yearString = match.group(1)
            year = int(yearString)
        return (movieString, year)

    #question 1.E
    def computeLongevousActor(self, maxYear):
        actorMinYearDict = {}
        actorMaxYearDict = {}
        for node in self.G.nodes().data():
            if (node[1]["type"] == 1 and node[1]["year"] != "Not found" and node[1]["year"] <= maxYear):
                for l in self.G.neighbors(node[0]):
                    if(l not in actorMinYearDict or node[1]["year"] < actorMinYearDict.get(l)):
                        actorMinYearDict.update({l : node[1]["year"]})

        for node in self.G.nodes().data():
            if (node[1]["type"] == 1 and node[1]["year"] != "Not found" and node[1]["year"] <= maxYear):
                for l in self.G.neighbors(node[0]):
                    if(l not in actorMaxYearDict or node[1]["year"] > actorMaxYearDict.get(l)):
                        actorMaxYearDict.update({l : node[1]["year"]})

        maxWorkPeriod = 0
        actorMax = None
        for a in actorMinYearDict.items():
            if maxWorkPeriod < (actorMaxYearDict.get(a[0]) - actorMinYearDict.get(a[0])):
                actorMax = a[0]
                maxWorkPeriod = actorMaxYearDict.get(a[0]) - actorMinYearDict.get(a[0])
        self.logger.info(f"the actor who worked for the longest period until {maxYear} is {self.idActorDict[actorMax]} with {maxWorkPeriod} years")

    def computeAllLongevousActor(self):
        for x in range (1930, 2030, 10):
            self.computeLongevousActor(x)


    #question 2.1
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
                    visited[nextNode] = visited[currentNode]+1
        #Gruob by distance to create dict of distance
        Bu = defaultdict(list)
        for key, value in visited.items():
            Bu[value].append(key)
        return Bu

    def computeDiameter(self, LCC, Bu):
        i = lb = max(Bu)
        ub = 2*lb
        while ub > lb:
            self.logger.info("entro nel while di computeDiameter")
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
            self.logger.info(f"the diameter of the graph relative to year {maxYear} is {d}")

    #question 3.I
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
        self.logger.info(f"the actor with most collaborations ({maxCollaborations}) is {maxCollabActor}")

    #question 4
    def buildActorGraph(self):
        movie_list = self.idMovieDict.keys()
        for i in movie_list:
            actors = list(self.G.neighbors(i))
            for j in combinations(actors, 2):
                if self.A.has_edge(*j):
                    self.A[j[0]][j[1]]['weight'] += 1
                else:
                    self.A.add_edge(j[0],j[1], weight = 1)

    def mostCollaboratingActors(self):
        max = 0
        idActor1 = -1
        idActor2 = -1
        for n1, n2 in self.A.edges:
            if max < self.A[n1][n2]["weight"]:
                max = self.A[n1][n2]["weight"]
                idActor1 = n1
                idActor2 = n2
        self.logger.info(f"the pair of actors who collaborated the most with {max} is {(self.idActorDict[idActor1], self.idActorDict[idActor2])}")



def main():
    obj = IMDBGraph()
    obj.extractData()
    obj.buildGraph()
    obj.computeAllLongevousActor()
    #obj.maxCollaborations()
    #obj.computeAllDiameters()
    obj.buildActorGraph()
    obj.mostCollaboratingActors()
    
if __name__=="__main__":
    main()

