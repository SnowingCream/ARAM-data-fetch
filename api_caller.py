import requests
import json
import time
    

# moved functionalities from api_functions_legacy.ipynb
class APICaller:

    def __init__(self):

        # these are USA-based URL. May need to adjust for other regions
        self.global_url = "https://americas.api.riotgames.com"
        self.region_url = "https://na1.api.riotgames.com" # only for Api function call 6
    
        self.cnt = 0 # count to handle api call limit: 100 calls per 2 min.

        # keep the api key in a separate file and read from it.
        with open("api_key.txt", "r") as f:
            values = f.readlines()
            self.api_key = values[0].strip()
    
    '''
    Helper function 1
    Behavior: handle maximum api call rate: 100 calls per 2 min.
    Usage: called inside fetch().
    Paramter: None
    Return: None
    '''
    def handle_call_rate(self):
        
        self.cnt += 1
        
        if self.cnt % 100 == 0:
            print(F"have called {self.cnt} apis: 2 min break starts") 
            time.sleep(120) 

    '''
    Helper function 2
    Behavior: general api call with given url -> return data in json format(dict)
    Usage: called inside other api call functions.
    Parameter: url for the api call
    Return: data from the call
    '''
    def fetch(self, url):

        response = requests.get(url)
        self.handle_call_rate()
    
        if response.status_code != 200:
            print(F"response status not 200: {response.status_code}")
            
            # occasionally api server throws this error code
            if response.status_code == 500:
                print("instant server error, trying again.")
                time.sleep(3)
                fetch(url)
                
        data = response.json()
        # print(data)        
        return data


    '''
    Api call function 1
    Behavior: get a player's account info from riot_id: summoner_name, tag
    Parameter: player name, player tag
    Return: data(json) containing 3 fields: puuid, gameName (player name), tagLine (player tag)
    '''
    def get_account_from_riot_id(self, summoner_name, tag):
        url = F"{self.global_url}/riot/account/v1/accounts/by-riot-id/{summoner_name}/{tag}?api_key={self.api_key}"
        data = self.fetch(url)
    
        return data


    '''
    Api call function 2
    Behavior: get a player's account info from puuid
    Parameter: player puuid
    Return: data(json) containing 3 fielids: puuid, gameName (player name), tagLine (player tag)
    
    '''
    def get_account_from_puuid(self, puuid):
        url = F"{self.global_url}/riot/account/v1/accounts/by-puuid/{puuid}?api_key={self.api_key}"
        data = self.fetch(url)
    
        return data


    '''
    Api call function 3
    Behavior: get a list of match records of given puuid
    Parameter: 
        puuid
        end_time (required to manipulate for following chain calls (in case of wanting more than 100 histories)
            - if end time is not 0, it is used to call the older records of the player.
            - more explanation necessary here
        queue_id (450 for ARAM)
        count = # records (20 is default, possible range = 0 ~ 100)
    Return: a list of match IDs
    
    '''
    def get_match_id_list_from_puuid(self, puuid, end_time = 0, queue_id = 450, count = 20):
    
        if end_time == 0:
            url = F"{self.global_url}/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={queue_id}&start=0&count={count}&api_key={self.api_key}"
        else:
            url = F"{self.global_url}/lol/match/v5/matches/by-puuid/{puuid}/ids?endTime={end_time}&queue={queue_id}&start=0&count={count}&api_key={self.api_key}"
            
        data = self.fetch(url)
        
        return data


    '''
    Api call function 4
    Behavior: get information about the given match
    Parameter: match ID
    Return: data(json): many information, details: https://developer.riotgames.com/apis#match-v5/GET_getMatch
    '''
    
    def get_match_from_match_id(self, match_id):
        url = F"{self.global_url}/lol/match/v5/matches/{match_id}?api_key={self.api_key}"
        data = self.fetch(url)
        return data

    
    '''
    Api call function 5
    Behavior: get information about the given match with timeline (much more detailed than Api call function 4)
    Parameter: match ID
    Return: data(json): many information, details: https://developer.riotgames.com/apis#match-v5/GET_getTimeline
    '''
    
    def get_timeline_from_match_id(self, match_id):
        url = F"{self.global_url}/lol/match/v5/matches/{match_id}/timeline?api_key={self.api_key}"
        data = self.fetch(url)
        return data


    '''
    Api call function 6
    Behavior: get a player account information from summoner ID
        - used to get a puuid of the user in a game prior to 13.23 patch(riot tag update)
        - then use puuid to find the current name and tag (Api function call 2).
        - since decided to not collect data before 14.1, this function won't be used.
    Parameter: summnor ID (can be found from Api can function 4)
    Return: data(json) containing 6 fields: id, accountId, puuid, profileIconId, revisionDate, summerLevel
        - most likely used to fetch puuid 
    '''
    
    def get_summoner_from_summoner_id(self, summoner_id):
        url = F"{self.region_url}/lol/summoner/v4/summoners/{summoner_id}?api_key={self.api_key}"
        data = self.fetch(url)
        return data


    '''
    Api call function 7
    Behavior: get a player account information from puuid
        - same behavior has Api function 6, just different input (puuid instead of summoner ID)
        - added to fetch profileIconId using puuid, which is a newly added DB. 
    Parameter: puuid
    Return: data(json) containing 6 fields: id, accountId, puuid, profileIconId, revisionDate, summerLevel
    '''
    
    def get_summoner_from_puuid(self, puuid):
        url = F"{self.region_url}/lol/summoner/v4/summoners/by-puuid/{puuid}?api_key={self.api_key}"
        data = self.fetch(url)
        return data




