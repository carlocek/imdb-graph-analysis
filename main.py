import networkx as nx
import pandas as pd
import re
import logging
from queue import Queue
from collections import defaultdict
from itertools import combinations

class IMDBGraph():

    def __init__(self):
        '''
        G: main graph, A: actor graph
        edgesDict: dict of dict where the keys of the outer dict are the index of the dataframe data
            and the values are dicts with keys "Actor" and "Movie" and values the relative entries of the dataframe
        actorIdDict: dict where the keys are the names of the actors and the values are the id
        movieIdDict: dict where the keys are the titles of the movies and the values are the id
        idActorDict and idMovieDict: reverse dictionaries
        movieYearDict: dict where the keys are the titles of the movies and the values are the relative years
        '''
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
        self.logger.info(f"bult object IMDBGraph")

    def extractData(self):
        '''
        uses pandas to extract data from tsv file and populate dictionaries
        '''
        data = pd.read_csv(self.imdbFilePath, sep="\t")
        #data = pd.read_csv(self.provaFilePath, sep="\t")
        self.edgesDict = data.to_dict("index")
        self.idActorDict = dict(enumerate(data["Actor"].unique()))
        self.actorIdDict = {v: k for k ,v in self.idActorDict.items()}
        self.idMovieDict = dict(enumerate(data["Movie"].unique(), start=len(self.idActorDict)))
        self.movieIdDict = {v: k for k, v in self.idMovieDict.items()}
        self.movieYearDict= dict(map(self.getYear,data["Movie"].unique()))
        self.logger.info(f"extracted data from file and created dictionaries")

    def buildGraph(self):
        '''
        uses networkx functions and list comprehensions to create graph nodes and edges
        with relative attributes
        type = 0 indicates an actor node
        type = 1 indicates a movie node
        '''
        self.G.add_nodes_from([(id, {"name": a, "type": 0}) for id, a in self.idActorDict.items()])
        self.G.add_nodes_from([(id, {"name": m, "type": 1, "year": self.movieYearDict[m]}) for id, m in self.idMovieDict.items()])
        self.G.add_edges_from([(self.actorIdDict[v["Actor"]], self.movieIdDict[v["Movie"]]) for v in self.edgesDict.values()])
        self.logger.info('built the main graph')

    def getYear(self, movieString):
        '''
        takes a movie title string from dataframe and returns a tuple
        with this string and the relative year extracted from it
        '''
        match = re.search(".*\(.*(\d\d\d\d).*\)", movieString)
        if match is None:
            year = "Not found"
        else:
            yearString = match.group(1)
            year = int(yearString)
        return (movieString, year)


    #question 1.E
    def computeLongevousActor(self, maxYear):
        '''
        for every film up to year "maxYear":
            for every actor that partecipates in that film:
                compute first and last work year
        for every actor
            compute working period
        find the actor who has worked for more years
        '''
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
        '''
        for every year x in {1930,1940,1950,1960,1970,1980,1990,2000,2010,2020}:
            find the longevous actor up to year x
        '''
        for x in range (1930, 2030, 10):
            self.computeLongevousActor(x)


    #question 2.1
    def customBFS(self, LCC, startNode):
        '''
        runs a BFS in the graph LCC from startNode and returns a dict
        Bu where keys are distances d and values are lists containing
        nodes at distance d from startNode
        '''
        visited = {}
        queue = Queue()
        queue.put(startNode)
        visited[startNode] = 0
        while not queue.empty():
            currentNode = queue.get()
            for nextNode in LCC.neighbors(currentNode):
                if nextNode not in visited:
                    queue.put(nextNode)
                    visited[nextNode] = visited[currentNode]+1
        Bu = defaultdict(list)
        for key, value in visited.items():
            Bu[value].append(key)
        return Bu

    def computeDiameter(self, LCC, Bu):
        '''
        implements the iFub algorithm
        '''
        i = lb = max(Bu)
        ub = 2*lb
        numBFS = 0
        while ub > lb:
            #self.logger.info(f"entro nel while di computeDiameter")
            eccDict = nx.eccentricity(LCC, Bu[i])
            numBFS += len(Bu[i])
            Bi = max(eccDict.values())
            maxVal = max(Bi,lb)
            if maxVal > 2*(i - 1):
                return (maxVal, numBFS)
            else: 
                lb = maxVal
                ub = 2*(i - 1) 
            i = i - 1 
        return (lb, numBFS)

    def computeAllDiameters(self):
        '''
        for every year x in {1930,1940,1950,1960,1970,1980,1990,2000,2010,2020}:
            extracts the subgraph containing only movie nodes with year prior to x
            extracts from this subgraph the largest connected components
            calculates the node with highest degree startNode
            evaluates the diameter starting from startNode
        '''
        for maxYear in range (1930, 2030, 10):
            SG = self.G.subgraph([n for n, a in self.G.nodes().data() if a["type"] == 0 or (a["year"] != "Not found" and a["year"] < maxYear)])
            tempLCC = max(nx.connected_components(SG), key=len)
            LCC = SG.subgraph(tempLCC)
            startNode = max(LCC.degree, key=lambda x: x[1])[0]
            Bu = self.customBFS(LCC, startNode)
            d, numBFS = self.computeDiameter(LCC, Bu)
            self.logger.info(f"the diameter of the graph relative to year {maxYear} is {d}: {numBFS} BFS executed")


    #question 3.I
    def maxCollaborations(self):
        '''
        iterates on the graph nodes considering only actor nodes,
        counts the collaborations done by every actor by iterating on its neighbors (films)
        and counting for every film the number of neighbors of that film.
        builds the dict where keys are actors id and values are the number
        of collaborations of that actor.
        returns the maximum number of collaborations with the relaive actor
        '''
        collaborations = {}
        for n, a in self.G.nodes().data():
            if a["type"] == 0:
                c = 0
                for nb in self.G.neighbors(n):
                    c += (len(self.G[nb])-1)
                collaborations.update({a["name"]: c})
        maxCollaborations = max(collaborations.values())
        maxCollabActor = [k for k, v in collaborations.items() if v == maxCollaborations][0]
        self.logger.info(f"the actor with most collaborations ({maxCollaborations}) is {maxCollabActor}")


    #question 4
    def buildActorGraph(self):
        '''
        builds the actor graph iterating on the movies and creating an edge 
        for every combination there is between any two actors that participated in that movie,
        checks if the edge already exists and increments the weight attribute of that edge.
        evaluates the pair of actor who did the most films together by checking the weight
        attribute of the edges as they are added to the graph
        '''
        maxCollab = 0
        a1 = None
        a2 = None
        for m in self.idMovieDict:
            if len(self.G[m]) > 1:
                actors = list(self.G.neighbors(m))
                for t in combinations(actors, 2):
                    if self.A.has_edge(*t):
                        self.A[t[0]][t[1]]['weight'] += 1
                        if self.A[t[0]][t[1]]['weight'] > maxCollab:
                            maxCollab = self.A[t[0]][t[1]]['weight']
                            a1, a2 = t
                    else:
                        self.A.add_edge(t[0],t[1], weight = 1)
        self.logger.info(f"built the actor graph")
        self.logger.info(f"the pair of actors who collaborated the most with is {(self.idActorDict[a1], self.idActorDict[a2])} with {maxCollab} collaborations")


def main():
    '''
    driver funcion
    '''
    obj = IMDBGraph()

    #creates dictionaries and builds the graph
    obj.extractData()
    obj.buildGraph()

    #answers question 1.E
    logging.info("QUESTION 1.E")
    obj.computeAllLongevousActor()

    #answers question 3.I
    logging.info("QUESTION 3.I")
    obj.maxCollaborations()

    #answers question 2.1
    logging.info("QUESTION 2.1")
    obj.computeAllDiameters()

    #answers question 4
    logging.info("QUESTION 4")
    #obj.buildActorGraph()
    
if __name__=="__main__":
    main()

