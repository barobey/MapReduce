# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 9:38:59 2018

@author: Brooke Robey
"""

from mrjob.job import MRJob

class TotalAmount(MRJob):
    
    def mapper(self, key, line):
        (customerID, orderID, amount) = line.split(',')
        amount = float(amount)
        yield customerID, amount

    def reducer(self, customerID, amount):
        yield customerID, sum(amount)

if __name__ == '__main__':
    TotalAmount.run()
    
# !python Part1.py DataA1.csv > totalamount.txt
