# IMDB graph analysis
This project was done in collaboration with my colleague [Luca Leuter](https://github.com/carlocek).

We used **pandas** and **networkx** to build a graph from a tsv file extracted from IMDB where each row is a pair of strings separated by a tab: the first string is the name of an actor and the second is the title of a film he/she participated in (in the title string there are also other informations like the release year).

We then answered some questions like:
* Which one is the actor who worked for the longest period (we computed the difference between the first and the last year he/she worked), considering only the movies up to year $x$ with $x \in \{1930,1940,1950,1960,1970,1980,1990,2000,2010,2020\}$?
* What is the diameter of the graph, considering only the movies up to year $x$ with $x \in \{1930,1940,1950,1960,1970,1980,1990,2000,2010,2020\}$ and restricting to the largest connected component?
* Who is the actor who had the largest number of collaborations?

Finally we built also the actor graph whose nodes are only actors and two actors are connected if they did a movie together. We used it to find the pair of actors who collaborated the most.
