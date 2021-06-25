
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

#########################################################################################################

all_ItemTable = []
all_CategoryTable = []
all_UserTable = []
all_BidTable = []
all_ids = []

def ItemTable(item):
    item_id = item['ItemID']
    name = item['Name'].replace('"', '""')
    
    currently = transformDollar(item['Currently'])
    buy_price = transformDollar(item['Buy_Price']) if "Buy_Price" in item else 'NULL'
    first_bid = transformDollar(item['First_Bid'])
    
    started = transformDttm(item['Started'])
    ends = transformDttm(item['Ends'])    
    
    description = item['Description'].replace('"', '""') if item['Description'] is not None else ""
    seller_id = item['Seller']['UserID']
    number_of_bids = item['Number_of_Bids']

    all_ItemTable.append('"' + '"|"'.join([item_id, name, currently, buy_price, first_bid,\
            number_of_bids, started, ends, description,seller_id]) + '"\n')

def CategoryTable(item):

    item_id = item['ItemID']
    for c in set(item['Category']):
        c_type = c.replace('"', '""')
        all_CategoryTable.append('"' + '"|"'.join([item_id, c_type]) + '"\n')


def BidTable(item):
    item_id = item['ItemID']
    
    if item['Bids'] is not None:
        for b in item['Bids']:
            bidder = b['Bid']['Bidder']
            bidder_id =bidder['UserID'].replace('"', '""')
            amount = transformDollar(b['Bid']['Amount'])
            time = transformDttm(b['Bid']['Time'])
            all_BidTable.append('"' + '"|"'.join([item_id, bidder_id,time,amount]) + '"\n')

def UserTable(item):

    seller_id = item['Seller']['UserID'].replace('"', '""')
    
    if seller_id not in all_ids:
        seller_rating = item['Seller']['Rating']
        seller_location = item['Location'].replace('"', '""')
        seller_country = item['Country'].replace('"', '""')
        
        all_UserTable.append('"' + '"|"'.join([seller_id, seller_rating, seller_location, seller_country]) + '"\n')
        all_ids.append(seller_id)

    if item['Bids'] is not None:
        for b in item['Bids']: 
            bidder = b['Bid']['Bidder']
            bidder_id = bidder['UserID'].replace('"', '""')
            if bidder_id not in all_ids:
                bidder_rating = bidder['Rating']
                bidder_location = bidder['Location'].replace('"', '""')if "Location" in bidder else 'NULL'
                bidder_country = bidder['Country'].replace('"', '""') if "Country" in bidder else 'NULL'
                all_UserTable.append('"' + '"|"'.join([bidder_id, bidder_rating, bidder_location, bidder_country]) + '"\n')
                all_ids.append(bidder_id)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
    
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        for item in items:
            ItemTable(item)
            CategoryTable(item)
            UserTable(item)
            BidTable(item)
            pass

def output():
    folder = ""

    with open(folder + "Item.dat","w") as f:
        f.write("".join(item for item in all_ItemTable)) 

    with open(folder + "Category.dat","w") as f: 
        f.write("".join(category for category in all_CategoryTable)) 

    with open(folder + "User.dat","w") as f: 
        f.write("".join(user for user in all_UserTable)) 

    with open(folder + "Bid.dat","w") as f: 
        f.write("".join(bid for bid in all_BidTable))





"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print("Success parsing " + f)
    output()

if __name__ == '__main__':
    main(sys.argv)