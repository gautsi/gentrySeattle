import matplotlib.pyplot as plt
import numpy as np
from sklearn.neighbors import KNeighborsClassifier


class NbdPred:
    """
    A neighborhood predictor class which takes as a parameter a list of places whose neighborhood is known. The predictor is nearest neighbor.  
    
    Parameters
    __________
    
    :param list loc_and_n: the data used to build a predictor, as a list of places. Each place is a list of two floats and a str; the two floats are the location of the place, and the str is the neighborhood the place belongs to.
    
    """
    
    def __init__(self, loc_and_n):
        
        self.loc_and_n = loc_and_n
        
        #Get the set of neighborhoods
        self.neighborhoods = set(zip(*loc_and_n)[2])

        #make a list of neighs (helpful for plotting and colors)
        self.neighborhoods_list = sorted(list(self.neighborhoods), reverse = True)
        
        #the number of neighborhoods
        self.num_neighborhoods = len(self.neighborhoods_list)
        
        #make a frequency dictionary
        self.nfreq = {n : 0 for n in self.neighborhoods}
        for r in self.loc_and_n:
            self.nfreq[r[2]] += 1
            
        #Assign a random color to each neighborhood
        self.neighborhood_colors = {n:map(lambda x : x*0.8, (np.random.random(), np.random.random(), np.random.random())) for n in self.neighborhoods}

        #Create color map
        self.ncmap = ListedColormap([self.neighborhood_colors[n] for n in self.neighborhoods_list])
        
        #Get the lats and longs
        self.latis = [r[0] for r in self.loc_and_n]
        self.longis =  [r[1] for r in self.loc_and_n]        
            
        #make a neighborhood predictor (nearest neighbor) and compute the classification rate
        self.NN, self.class_rate = self.make_predictor()
        
        
        
    def make_predictor(self):
        """
        Split the data set into training and test sets, return a nearest neighbor predictor trained on the training set and the classification rate on the test set.
        
        Returns
        _______
        
        :return: a nearest neighbor predictor and its classification rate
        :rtype: sklearn.neighbors.KNeighborsClassifier
        
        """
        
        #divide the data into test and train: 90% train, 10% test
        datasize = len(self.loc_and_n)
        train_data_indices = []
        
        #at least one point from each neigh should be in the train set
        for n in self.neighborhoods:
            #get the indices in loc_and_n of the points with neigh n
            indices = [self.loc_and_n.index(place) for place in self.loc_and_n if place[2] == n]
            train_data_indices += list(np.random.choice(indices, 1))
        
        #fill in the rest of the train data
        l = [ind for ind  in xrange(len(self.loc_and_n)) if not ind in train_data_indices]
        s = int(datasize*0.9 - len(train_data_indices))
        train_data_indices += list(np.random.choice(a = l, size = s, replace = False))
        
        #make the train and test data sets
        train_data = [self.loc_and_n[ind] for ind in train_data_indices]
        
        test_data_indices = [ind for ind in xrange(len(self.loc_and_n)) if not ind in train_data_indices]
        test_data = [self.loc_and_n[ind] for ind in test_data_indices]
        
        #train a nearest neighbor classifier
        NN = neighbors.KNeighborsClassifier(n_neighbors=1)
        NN.fit([place[:2] for place in train_data], [place[2] for place in train_data])
        
        #what's the classification rate on the test set?
        class_rate = sum([NN.predict(place[:2]) == place[2] for place in test_data])/float(len(test_data))
        
        return NN, class_rate
    
    def plot_decision_regions(self, points = True):
        xx, yy = np.meshgrid(np.arange(min(self.longis), max(self.longis), 0.0005), np.arange(min(self.latis), max(self.latis), 0.0005))
        Z = np.array(map(lambda n: self.neighborhoods_list.index(n), self.NN.predict(zip(yy.ravel(), xx.ravel()))))
        Z = Z.reshape(xx.shape)
        c = [self.neighborhoods_list.index(r[2]) for r in self.loc_and_n]
        plt.pcolormesh(xx,yy,Z, cmap = plt.get_cmap("Paired"))
        if points:
            plt.scatter(self.longis, self.latis, c = c, cmap = plt.get_cmap("Paired"))

        cbar = plt.colorbar(ticks = range(self.num_neighborhoods))
        cbar.ax.set_yticklabels(self.neighborhoods_list)
        plt.show()
        
        
