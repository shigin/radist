import warnings
from basenode import RadistNode
from errors import *
from attrs import *

class ColoredNode(RadistNode):
    def __init__(self, name):
        RadistNode.__init__(self, name)
        self.__colors = {}
        self.__last = 0

    def __repr__(self):
        return "<ColoredNode '%s' cluster: %d colors: %d>" % \
                (self.name, self.cluster_size(), len(self.__colors))

    def add_child(self, node, name=None):
        name = name or node.name
        if name.isdigit():
            if name not in self.__colors:
                ncolor = RadistNode('color' + name)
                self.__colors[name] = ncolor
                RadistNode.add_child(self, ncolor)
            self.__last += 1
            self.__colors[name].add_child(node, "%02d" % self.__last)
            return RadistNode.add_child(self, node, "%04d" % self.__last)
        else:
            return RadistNode.add_child(self, node, name)
