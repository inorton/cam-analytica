# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 08:45:50 2016
@author: Michael
For starters, we will just get sentiment from textBlob for tweets containing keywords like "Trump", "Carson", "Cruz", "Bern", Bernie", "guns", "immigration", "immigrants", etc.  
matplotlib the results. 
Stuff to do: 
    Get user IDs
    retrieve all the user's recent tweets and favorites. 
    separate tweets into groups containing each keyword
    get sentiment graph of the whole group with textBlob and matplotlib
    
"""



#Import the necessary methods from tweepy library
import sys
import tweepy
from tweepy import OAuthHandler
import textblob
import numpy as np
import time
import pandas
from tweepy.streaming import StreamListener
from tweepy import Stream

def readFromFileA(filename,splitter=',', lineStart = 0, lineEnd = 1000):
    f = open(filename,'r')
    lines_list = f.readlines()
    f.close()
    my_data = [[str(val) for val in line.split(splitter)[0:]] for line in lines_list[lineStart:lineEnd]]    
    my_data = filter(lambda a: a != ['\n'], my_data)
    return my_data
    
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        randIds.append(status.user.id)




#input is a tweet as a single line str.  This function will convert it all to lower case, remove useless words, and put in the format for the neural network.  
#def parseTweet(tweet):    #will make this when I'm working on the nn part of the project.  not needed if using the textblob tool
    
    




if __name__ == '__main__':
    #Variables that contains the user credentials to access Twitter API 
    consumerKey = 'BSfiAQWf44tc7AvLeMfMqVFld'
    consumerSecret = 'Nk0x66OaUrHNn4WjC1KbCP3kUoUe1KvH6dMtgm96IWstzl32Jq'
    accessToken = '324214621-bkOFZdKv1X9Rd9pTI6TCk3bsFuAvL1ug2ozZFlKc'
    accessTokenSecret = 'grLo38PqPDQyj7dcJH7XlsfRTaqBLtLwaADBPpgaUDwZb'
    
    
    idsFileName = "./data/twitter_ids.csv"
    dataFileName2 = "./data/Video Transcript.txt"
    randIdsFileName = "./data/randomIds.txt"
    

    auth = OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)
    api = tweepy.API(auth)
    ids = readFromFileA(idsFileName, splitter = ',', lineStart = 1, lineEnd = 3000)
    
    
    """if we are using a randomized control group, this code creates the ids list for them."""
    #if getting randoms
#    ids = []
#    with open(randIdsFileName, 'r') as f:
#        for line in f:
#            ids.append(int(line))
#    
#    ids = ids[:720]
    
    
    """this is only for non-tweet files, such as focus group transcripts"""
#    lines = []
#    
#    with open(dataFileName2, 'r') as f:
#        for line in f:
#            lines.append(line.lower())
    
    
    """this is the tweet id of roughly the time that the news broke that Carson dropped out.  I pull only tweets since that time"""
    carsonDropsOutTweetId = 705885709861715968
    
    
    
    """hard coded lists for keywords that I'm searching for.   This is used to compile data for sentiments regarding tweets containing each keyword."""
    
    
    #each sentiments list will have tuples: (sentiment, tweetID)
    #note: could include many more keywords like "feelthebern" for example, but need neutral keywords to get true sentiments.  feelthebern would be a biased term.  
    hillarySentiments = []
    hillaryKeywords = ['hillary', 'clinton', 'hillaryclinton']
    trumpSentiments = []
    trumpKeywords = ['trump', 'realdonaldtrump']
    cruzSentiments = []
    cruzKeywords = ['cruz', 'tedcruz']
    bernieSentiments = []
    bernieKeywords = ['bern', 'bernie', 'sanders', 'sensanders']
    obamaSentiments = []
    obamaKeywords = ['obama', 'barack', 'barackobama']
    republicanSentiments = []
    republicanKeywords = ['republican', 'conservative']
    democratSentiments = []
    democratKeywords = ['democrat', 'dems', 'liberal']
    gunsSentiments = []
    gunsKeywords = ['guns', 'gun', 'nra', 'pistol', 'firearm', 'shooting']
    immigrationSentiments = []
    immigrationKeywords = ['immigration', 'immigrants', 'citizenship', 'naturalization', 'visas']
    employmentSentiments = []
    emplyomentKeywords = ['jobs', 'employment', 'unemployment', 'job']
    inflationSentiments = []
    inflationKeywords = ['inflate', 'inflation', 'price hike', 'price increase', 'prices rais']
    minimumwageupSentiments = []
    minimumwageupKeywords = ['raise minimum wage', 'wage increase', 'raise wage', 'wage hike']
    abortionSentiments = []
    abortionKeywords = ['abortion', 'pro-choice', 'planned parenthood']
    governmentspendingSentiments = []
    governmentspendingKeywords = ['gov spending', 'government spending', 'gov. spending', 'expenditure']
    taxesupSentiments = []
    taxesupKeywords = ['raise tax', 'tax hike', 'taxes up', 'tax up', 'increase taxes', 'taxes increase', 'tax increase']
    taxesdownSentiments = []
    taxesdownKeywords = ['lower tax', 'tax cut', 'tax slash', 'taxes down', 'tax down', 'decrease taxes', 'taxes decrease', 'tax decrease']
    
    
    #(nameOfTuple, sentimentList, keywordList)
    personSentimentList = [('hillary', hillarySentiments, hillaryKeywords), ('trump', trumpSentiments, trumpKeywords), ('cruz', cruzSentiments, cruzKeywords), 
                           ('bernie', bernieSentiments, bernieKeywords), ('obama', obamaSentiments, obamaKeywords)]
    issueSentimentList = [('guns', gunsSentiments, gunsKeywords), ('immigration', immigrationSentiments, immigrationKeywords), 
                          ('employment', employmentSentiments, emplyomentKeywords), ('inflation', inflationSentiments, inflationKeywords),
                          ('minimum wage up', minimumwageupSentiments, minimumwageupKeywords), ('abortion', abortionSentiments, abortionKeywords),
                          ('government spending', governmentspendingSentiments, governmentspendingKeywords), ('taxes up', taxesupSentiments, taxesupKeywords), 
                          ('taxes down', taxesdownSentiments, taxesdownKeywords) ]
     


    
"""this bit is for taking random twitter IDs for the control group.  It simply skims the most recent tweets that have mentioned one of our keywords.
   it turned out that skimming all of the tweets found very very few occurances of keywords since twitter is such a global/multilingual platform"""
    
    #randIds = []
    
#    allKeys = []
#    for person in personSentimentList:
#        for keyWord in person[2]:
#            allKeys.append(keyWord)
#    for issue in issueSentimentList:
#        for keyWord in issue[2]:
#            allKeys.append(keyWord)
#    
#    myStreamListener = MyStreamListener()
#    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
#    sys.exit()
#    myStream.filter(track = allKeys)
            
            
            
"""here is the format for the basic loop for finding text that has the keywords we're searching for.
   It then finds the sentiment and adds that to the respective keywords' data list
   This particular segment uses 'lines' which is for if we're looking through non-tweets (like transcripts of some sort""" 
            
#    for line in lines:
#        for person in personSentimentList:
#            for keyword in person[2]:
#                if keyword in line:
#                    try:
#                        tb=textblob.TextBlob(line)
#                        person[1].append((tb.sentiment.polarity, 5))
#                        break
#                    except:
#                        continue
#                    
#    for line in lines:
#        for person in issueSentimentList:
#            for keyword in person[2]:
#                if keyword in line:
#                    try:
#                        tb=textblob.TextBlob(line)
#                        person[1].append((tb.sentiment.polarity, 5))
#                        break
#                    except: 
#                        continue
            
            
     
     
    """this big block goes through tweets of each user, looks for keywords, and if the keyword is there, 
       we find the sentiment for that tweet and add it to the sentiment data list"""
    start = time.time()
    try: 
        ids = np.asarray(ids)[:,1]
    except:
        ids = np.asarray(ids)
    try:
        ids = ids.astype(np.int)
    except:
        print "whoops"
    i = 0
    counter = 0
    totalIdsWithMentions = 0
    mentionFlag = False
    for idno in ids:
        try:
            idno = int(idno)
        except:
            print 'idno too long to convert to int'
        if mentionFlag == True:
            totalIdsWithMentions = totalIdsWithMentions + 1
        mentionFlag = False
        
        """the rate limit is handled here.  Also, if for some reason we can't access the tweets (like internet failure)
           we don't want to crash, so we wait 30 seconds and try again.""" 
        if i % 2 == 0:
            try:
                apiInfo = api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline']
            except:
                print 'no internet, sleeping for 30 seconds'
                time.sleep(30)
            if apiInfo['remaining'] < 2:
                timeToSleep = apiInfo['reset'] - time.time()
                if timeToSleep > 0:
                    print 'sleeping for: ', timeToSleep, ' seconds'
                    sys.stdout.flush()
                    time.sleep(timeToSleep + 1)
                else:
                    time.sleep(1)
                          
        if i % 100 == 0:
            print "on id number: ", i
            sys.stdout.flush()
        i = i + 1
        counter = counter + 1
        try:
            for status in tweepy.Cursor(api.user_timeline, user_id = idno, since_id = carsonDropsOutTweetId).items(20):
                statusText = status.text.lower()
                for person in personSentimentList:
                    for keyword in person[2]:
                        if keyword in statusText:
                            tb = textblob.TextBlob(statusText)
                            person[1].append((tb.sentiment.polarity, status.id))
                            mentionFlag = True
                            break
                for issue in issueSentimentList:
                    for keyword in issue[2]:
                        if keyword in statusText:
                            tb = textblob.TextBlob(statusText)
                            issue[1].append((tb.sentiment.polarity, status.id))
                            mentionFlag = True
                            break
        except KeyboardInterrupt:
            raise
        except:
            print sys.exc_info()[0]
            sys.stdout.flush()
            counter = counter - 1
            continue
        
     
    arrayList = []             
                    
    
    
    """ here we're just compiling the sentiment data for each keyword group into an easier to work with format (dataframe).
        df will contain the mean and median and mention count data.  Note that it is only meaningful if compared with a 
        control group, since keyword selection is impossible to employ neutrally.  """
    
    for person in personSentimentList:
        sentimentData = np.asarray(person[1])
        if len(sentimentData) > 0:
            arrayList.append([person[0], np.mean(sentimentData[:,0]), np.percentile(sentimentData[:,0], 50), len(sentimentData)] )
            
    for issue in issueSentimentList:
        sentimentData = np.asarray(issue[1])
        if len(sentimentData) > 0:
            arrayList.append([issue[0], np.mean(sentimentData[:,0]), np.percentile(sentimentData[:,0], 50), len(sentimentData)])
    
    meanMedianCountData = np.asarray(arrayList)
    df = pandas.DataFrame(meanMedianCountData, columns=['name', 'mean', 'median', 'count'])
    df[['name']] = df[['name']].astype(str)
    df[['mean', 'median']] = df[['mean', 'median']].astype(float)
    df[['count']] = df[['count']].astype(int)
    
    df.sort(['count'], ascending = 0, inplace = True)
    
    print df
    
    print 'time taken: ' , time.time()- start
    print 'number of ids read: ' , counter
    print 'Number of ids with keyword mentions: ' , totalIdsWithMentions
    
    

    
    
        
        
    
    
    
    
    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
    
