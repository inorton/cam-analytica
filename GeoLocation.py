# -*- coding: utf-8 -*-
"""
Created on Thur Mar 17 9:21:35 2016
@author: Michael Phillips    
@email: michael.philips@CambridgeAnalytica.org
Script's main purpose is to complete an array of addresses with accurate latitudes and longitudes using the completeAddress function
Includes another function compareAPItoSource for testing APIs with source latitude longitudes.  
More detailed comments are above and within each primary function.  Utility functions are fairly self explanatory.  
"""

import numpy as np
import pandas as pd
from googlemaps import Client
import googlemaps as gmaps
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import geocoder
from random import random
from time import sleep
from geopy.distance import vincenty
import scipy.stats
from collections import OrderedDict
import sys
import time



##################
#UTILITY FUNCTIONS
##################
def readFromFileA(filename,splitter=',', lineStart = 0, lineEnd = 1000):
    f = open(filename,'r')
    lines_list = f.readlines()
    f.close()
    my_data = [[str(val) for val in line.split(splitter)[0:]] for line in lines_list[lineStart:lineEnd]]    
    my_data = filter(lambda a: a != ['\n'], my_data)
    data = np.asarray(my_data)
    return data
    
def getLatLngGoog(client, addr):
    location = gmaps.geocoding.geocode(client, addr)[0].get('geometry').get('location')
    lat = location.get('lat')
    lng = location.get('lng')
    return lat,lng

def getDistance(latlng1, latlng2):
    return vincenty(latlng1, latlng2).miles
    
def getLatLngGeopy(key, address):  ## key is not used
    geolocator = Nominatim()
    #address =  (house_num + " " + street_name + " " + city + ", " + state_abrv + " " + zip_code) 
    sleep(0.1 + 0.2 *random())
    location = geolocator.geocode(address)
    if location:
        return (location.latitude, location.longitude)
    else:
        return 0
        
def getLatLngArcGIS(key, address):   ##key is not used.  Only present to support googleMaps.  
    g = geocoder.arcgis(address)
    if g.latlng == []:
        return 0
    h = [str(g.latlng[0]), str(g.latlng[1])]
    return h
    

"""
This Function is meant as a test function used to determine accuracy of source lng/lat data. 
_client is for googlemaps when the APIkey is needed.  Pass it something like client for:
    api_keyServer = 'AIzaSyA-Qh3hS_6uZjCXsayf53m-1-Q3h-5KEPo'
    client = Client(api_keyServer)
    
APIfunction is a function that takes a client and an addr as parameters (see getLatLngArcGIS(key, address) as the recommended option)
_fullAddresses is a list of addresses where each line is: 
    , address_id, voter_id, AddressLine, ExtraAddressLine, HouseNumber, PrefixDirection, StreetName, Designator, SuffixDirection, ApartmentNum, Zip, ZipPlus4, City, County, CongressionalDistrict, State, latitude, longitude, ar_latitude, ar_longitude 
    only columns 5, 7, 8, 13, 16, 17, 18, 19, 20 are required, the rest can be blank.  
    
    probabilities are printed out as "prob: [(x miles, probability of being within x miles),...]" where we're comparing Aristotle and IG data to the values fetched by the APIfunction. 
    
    The function returns how long it took in seconds to complete.  This may be useful for comparing APIs to each other. 
"""
def compareAPItoSource(_client, APIfunction, _fullAddresses):   
    
    
    start_time = time.time()
    lng_lat_pairs_API = []   
    lng_lat_pairs_ar = []
    lng_lat_pairs_ig = []
    exceptionCount = 0
    getllfails = 0
    lineCount = 0
    for line in _fullAddresses:
        lineCount = lineCount + 1
        sys.stdout.flush()
        if lineCount % 500 == 0:
            print "line count: " + str(lineCount)
        try:
            address = str(line[5] + " " + line[7] + " " + line[8] + " " + line[13] +  ", " + line[16])
            ll = APIfunction(_client, address)
            if ll == 0:
                getllfails = getllfails + 1
                continue
            else:
                lng_lat_pairs_API.append(ll)
                lng_lat_pairs_ig.append((line[17], line[18]))
                lng_lat_pairs_ar.append((line[19], line[20]))
            
            
        except:
            exceptionCount = exceptionCount + 1
            continue
    
    try:      
        lng_lat_pairs_API = np.array(lng_lat_pairs_API)
        lng_lat_pairs_ar = np.array(lng_lat_pairs_ar)
        lng_lat_pairs_ig = np.array(lng_lat_pairs_ig)
        APIzip = zip(lng_lat_pairs_API[:, 0], lng_lat_pairs_API[:, 1])
        arzip = zip(lng_lat_pairs_ar[:, 0], lng_lat_pairs_ar[:, 1])
        igzip = zip(lng_lat_pairs_ig[:, 0], lng_lat_pairs_ig[:, 1])
    except:
        print "Failure to complete " 
        return(time.time() - start_time)
    ardistanceList = []
    igdistanceList = []
    i = 0
    for pair in APIzip:
        ardistanceList.append(getDistance(pair, arzip[i]))
        i = i + 1  
    ardistanceNp = np.array(ardistanceList)
    i = 0
    for pair in APIzip:
        igdistanceList.append(getDistance(pair, igzip[i]))
        i = i + 1  
    igdistanceNp = np.array(igdistanceList)
    
    arbuckets = {.01: float(0), .1: float(0), 1: float(0), 10: float(0), 100: float(0)}
    for dis in ardistanceNp:
        if dis < .01:
            arbuckets[.01] = arbuckets[.01] + 1
        if dis < .1:
            arbuckets[.1] = arbuckets[.1] + 1
        if dis < 1:
            arbuckets[1] = arbuckets[1] + 1
        if dis < 10:
            arbuckets[10] = arbuckets[10] + 1
        if dis < 100:
            arbuckets[100] = arbuckets[100] + 1   
    arorderedBuckets = OrderedDict(sorted(arbuckets.items(), key=lambda t: t[0]))
    arprintBuckets = []
    for k,v in arorderedBuckets.iteritems():
        value = v/float(len(ardistanceNp))
        value = int((value * 100) + 0.5) / 100.0  #rounding to 2 decimals
        arprintBuckets.append((k, value))
        
    igbuckets = {.01: float(0), .1: float(0), 1: float(0), 10: float(0), 100: float(0)}
    for dis in igdistanceNp:
        if dis < .01:
            igbuckets[.01] = igbuckets[.01] + 1
        if dis < .1:
            igbuckets[.1] = igbuckets[.1] + 1
        if dis < 1:
            igbuckets[1] = igbuckets[1] + 1
        if dis < 10:
            igbuckets[10] = igbuckets[10] + 1
        if dis < 100:
            igbuckets[100] = igbuckets[100] + 1   
    igorderedBuckets = OrderedDict(sorted(igbuckets.items(), key=lambda t: t[0]))
    igprintBuckets = []
    for k,v in igorderedBuckets.iteritems():
        value = v/float(len(igdistanceNp))
        value = int((value * 100) + 0.5) / 100.0  #rounding to 2 decimals
        igprintBuckets.append((k, value))
    
    print "******Overall run stats*******"     
    print "get lat/lng fails: " + str(getllfails)
    print "number of other exceptions: " + str(exceptionCount)
    print "total number of successes " + str(len(ardistanceNp))
    
    
    print "*******Aristotle Data******" 
    print "mean distance: " + str(np.mean(ardistanceNp))
    print "probs: " + str(arprintBuckets)
    
    print "*******IG Data******" 
    print "mean distance: " + str(np.mean(igdistanceNp))
    print "probs: " + str(igprintBuckets)
    
    return(time.time() - start_time)
    
    
"""
This function is really the purpose of this script. Essentially what it does is: 
For each address in the addresses file, try to get an accurate lng/lat quickly (comparing available data
from Aristotle/IG to the zip code file data to determine accuracy), but if we can't, we fetch it from ArcGIS.
addresses is an array of addresses each in the form 
    , address_id, voter_id, AddressLine, ExtraAddressLine, HouseNumber, PrefixDirection, StreetName, Designator, SuffixDirection, ApartmentNum, Zip, ZipPlus4, City, County, CongressionalDistrict, State, latitude, longitude, ar_latitude, ar_longitude
    only lines 5, 7, 8, 13, 16 are used though, the rest can be blank.  
    lines 17,18,19,20 are optional, they are the data from Aristotle and IG lat/lng data.
    
_zips is an array of zip codes in the form:
zip, city, state, latitude, longitude, timezone, dst
    where latitude and longitude correspond to the center of the zip code. 
    note that zip codes should be in the same format as provided by the addresses file.  sometimes this means trimming leading zeroes.  
    
latlngFunc is the function you want to use to fetch lat/lngs that are not supplied in the addresses array.  
    getLatLngArcGIS is recommended due to accuracy and the fact that there is no usage restriction.  
    
The function returns an array which adds 2 extra columns to the original addresses array.  The extra columns are the accurate lat/lngs. 
Output is a little confusing, but the important bits are the fetch rate (basically dictates how quickly the function goes),
    and the errors (which is going to be the number of address lines which we couldn't get accurate data for from any source.)
********THINGS THAT CAN BE ADDED*************
Right now the exception clause basically just says that the latlngFunc failed to find the address and give a valid lat/lng.
    Instead, it could use googleMaps to fill in the field.  It would have to be limited to 2500 uses in a day, but at the average hit rate for 
    ArcGIS, it would be difficult for it to fail 2500 times in a single day.  This would increase coverage to possibly 100%, but if we did 
    hit our googleMaps fetch limit, it could cause the program to crash or take virtually forever.  
    
  
"""
def completeAddresses(addresses, _zips, latlngFunc):
    completeAddrs = np.hstack((addresses, np.zeros([len(addresses), 2])))
    completeAddrs = pd.DataFrame(completeAddrs)
    #create dictionary for zip lookups
    zipsList = _zips[:,0].tolist()
    latitudeList = _zips[:,3]
    longitudeList = _zips[:,4]
    latlngList = zip(latitudeList, longitudeList)
    zipsDict = dict(zip(zipsList, latlngList))
    
    radius = 15
    
    
    errorCount = 0
    igHitNumber = 0
    igMissNumber = 0
    arHitNumber = 0
    arMissNumber = 0
    numberOfFetches = 0
    
    for index, line in completeAddrs.iterrows():
        if int(line[0]) % 100 == 0:
            print line[0]
        try:
            #create address for future use
            address = str(line[5] + " " + line[7] + " " + line[8] + " " + line[13] +  ", " + line[16])
            #if no lat/lng present:
            if len(line[17]) + len(line[19]) == 0:
                #fetch lat/lng and plug it into completeAddrs
                (line[21],line[22]) = latlngFunc(19, address)
                numberOfFetches = numberOfFetches + 1
            #elif best source is available:
            elif len(line[17]) > 0:
                #compare to zipcode
                zipcodeFromFile = line[11]
                ziplatlng = zipsDict.get(zipcodeFromFile)
                dist = getDistance(ziplatlng, (line[17], line[18]))
                #if reasonable:
                if dist < radius:
                    #put it into completeAddrs
                    igHitNumber = igHitNumber + 1
                    (line[21], line[22]) = (line[17], line[18])
                #elif second best source is available:
                elif len(line[19]) > 0:
                    igMissNumber = igMissNumber + 1
                    #compare to zipcode
                    zipcodeFromFile = line[11]
                    ziplatlng = zipsDict.get(zipcodeFromFile)
                    dist = getDistance(ziplatlng, (line[17], line[18]))
                    #if reasonable:
                    if dist < radius:
                        arHitNumber = arHitNumber + 1
                        #put it into copmleteAddrs
                        (line[21], line[22]) = (line[19], line[20])
                        #else:
                    else:
                        arMissNumber = arMissNumber + 1
                        #fetch lat/lng and plug it into completeAddrs
                        (line[21],line[22]) = latlngFunc(19, address)
                        numberOfFetches = numberOfFetches + 1
            #else if second best source is available
            elif len(line[19]) > 0:
                #compare to zipcode
                zipcodeFromFile = line[11]
                ziplatlng = zipsDict.get(zipcodeFromFile)
                dist = getDistance(ziplatlng, (line[19], line[20]))         
                #if reasonable:
                if dist < radius:
                    arHitNumber = arHitNumber + 1
                    #put it into copmleteAddrs
                    (line[21], line[22]) = (line[19], line[20])
                    #else:
                else:
                    arMissNumber = arMissNumber + 1
                    #fetch lat/lng and plug it into completeAddrs
                    (line[21],line[22]) = latlngFunc(19, address)
                    numberOfFetches = numberOfFetches + 1
        except:      ### TypeError probably means that the latlngFunc returned 0.
#            print "exception: ", sys.exc_info()[0]
#            print "at line of addresses file: " , line[0]
            errorCount = errorCount + 1
            continue
        
    print "errors: " , errorCount
    print "Number of hits for IG: " , igHitNumber
    print "Number of misses for IG: " , igMissNumber
    print "Number of hits for Aristotle: " , arHitNumber
    print "Number of misses for Aristotle: " , arMissNumber
    print "Number of fetches from API: " , numberOfFetches
    print "Fetch %: ", float(numberOfFetches) / len(completeAddrs)
    return completeAddrs
    

    
"""
This right now assigns the completedAddresses array to the variable x, which can then be written to a different csv file or something if you want. 
Commented out at the bottem are some API tests.  Line by line comments are provided in the main script.  
"""
if __name__ == "__main__":
    #keys for googleMaps client.  can be set to whatever and commented out if not using that API. 
    api_keyBrowser = 'AIzaSyAnrOVz4qbOZveNHty6Yx8s-E_6BDzHRTk'
    api_keyServer = 'AIzaSyA-Qh3hS_6uZjCXsayf53m-1-Q3h-5KEPo'
    
    #creates the googlemaps client.  should be commented out if the googlemaps API isnt being used.  
    client = Client(api_keyServer)
    
    #file path for the address file
    dataFileName = "./data/address_nh.csv" 
    #file path for the zip code file 
    zipsFileName = "./data/zipcode.csv"
    
    #simple timer to check run speed    
    start = time.time()
    print "reading file..."
    
    #readFromFile lineStart should be 1 if the first line is a key, 0 if the first line is data to be used. lineEnd should be length of the file + 1 if you want it all 
    addresses = readFromFileA(dataFileName, splitter=',', lineStart = 40000, lineEnd = 41000)
    #readFromFile lineStart should be 1 if the first line is a key, 0 if the first line is data to be used. lineEnd should be length of the file + 1 if you want it all 
    zips = readFromFileA(zipsFileName, splitter=',', lineStart = 1, lineEnd = 1392)
    
    
    #get rid of apostrophes without corrupting file.  
    #    had to include because zip file I was given corrupted when I did a find and replace within the actual text file.  
    i = 0
    for line in zips:
        j = 0
        for col in line:
            zips[i][j] = col.replace("\"", "")
            j = j + 1
        i = i + 1
    
    
    #remove lead zeros so the dictionary within completeAddresses() works.  Can be altered depening on formatting of zip and addresses files.  just make sure they match.  
    i = 0
    for line in zips:
        zips[i][0] = str(line[0]).lstrip("0")
        i = i + 1
        
        
    #############
    #lat/lng filler script
    #############
    x = completeAddresses(addresses, zips, getLatLngArcGIS)
    #################
    
    
    
    ########################
    #API comparisons Script
    ######################
    #Set fullAddresses to addresses for which we have both aristotle and ig data
#    fullAddresses = np.empty((0,21))
#    
#    print "finding suitable addresses..." 
#    for line in addresses:
#        if len(line[17]) > 0 and len(line[19]) > 0:
#            fullAddresses = np.vstack((fullAddresses, line))
#            if len(fullAddresses) == 5000:
#                break
        
#    print "-------GEOPY-------"         
#    print "time taken: " + str(compareAPItoSource(client, getLatLngGeopy, fullAddresses))   ###client is not used
    
#    print "-------ArcGIS------"         
#    print "time taken: " + str(compareAPItoSource(client, getLatLngArcGIS, fullAddresses))
#    sys.stdout.flush()
    
#    print "-----GoogleMaps----"
#    print "time taken: " + str(compareAPItoSource(client, getLatLngGoog, fullAddresses))
#    sys.stdout.flush()
#    
    ###########################################
    
    
    print time.time() - start
    
    
    ################################
    #compare the API results to googlemaps results
    ################################
    
    
        
    

    
            
        
Contact GitHub 
