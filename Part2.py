# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 10:00:09 2018

@author: Brooke Robey
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class TotalAmountSorted(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_building_keys,
                   reducer=self.reducer_summing_amounts),
            MRStep(mapper=self.mapper_building_keys2,
                   reducer = self.reducer_output)
        ]
        
    def mapper_building_keys(self, key, line):
        (customerID, orderID, amount) = line.split(',')
        amount = float(amount)
        yield customerID, amount

    def reducer_summing_amounts(self, customerID, amount):
        yield customerID, sum(amount)

    def mapper_building_keys2(self, customerID, amount):
        yield '%04.02f'%float(amount), customerID

    def reducer_output(self, amount, customerIDs):
        for customerID in customerIDs:
            yield amount, customerID

if __name__ == '__main__':
    TotalAmountSorted.run()

# !python Part2.py DataA1.csv > totalamountsorted.txt