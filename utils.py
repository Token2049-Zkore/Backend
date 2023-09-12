import tweepy
import os
from training import model

from dotenv import load_dotenv
load_dotenv()

def get_twitter_score(user_name:str):
    # API keyws that yous saved earlier
    api_key = os.getenv("API_KEY")
    api_secrets = os.getenv("API_SECRET")
    access_token = os.getenv("ACCESS_TOKEN_KEY")
    access_secret = os.getenv("ACCESS_TOKEN_SCECRET")

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(api_key,api_secrets)
    auth.set_access_token(access_token,access_secret)

    api = tweepy.API(auth)
    try:
        api.verify_credentials()
        print('Successful Authentication')
    except:
        print('Failed authentication')

    # user_name = "CryptomeriaCap" # put 
    # _id = "103770785"
    #user = api.get_user(id = _id)
    user =api.get_user(screen_name = user_name)

    return [user.followers_count, user.listed_count, user.statuses_count]

def generate_proof(features : list):
    public, proof = model.predict(features)
    return public, proof

